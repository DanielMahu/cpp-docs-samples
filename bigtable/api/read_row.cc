// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "parse_taq_line.h"

#include <google/bigtable/admin/v2/bigtable_table_admin.grpc.pb.h>
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/protobuf/text_format.h>
#include <google/rpc/status.pb.h>
#include <grpc++/grpc++.h>

#include <ciso646>
#include <fstream>
#include <sstream>
#include <thread>

namespace {
// ... save ourselves some typing ...
namespace bigtable = ::google::bigtable::v2;

// Append a set of quotes to a mutate request
void append_to_request(bigtable::MutateRowsRequest& req,
                       std::string const& yyyymmdd,
		       Quotes const& quotes);

// Perform a Bigtable::MutateRows() request until all mutations complete.
void mutate_with_retries(bigtable::Bigtable::Stub& bt_stub,
                         bigtable::MutateRowsRequest& req);

// Create a table in the given project and instance
std::string create_table(std::shared_ptr<grpc::ChannelCredentials> creds,
                         std::string const& project_id,
                         std::string const& instance_id,
                         std::string const& table_id);

}  // anonymous namespace

int main(int argc, char* argv[]) try {
  // ... a more interesting application would use getopt(3),
  // getopt_long(3), or Boost.Options to parse the command-line, we
  // want to keep things simple in the example ...
  if (argc != 5) {
    std::cerr
        << "Usage: create_table <project_id> <instance_id> <filename> <date>"
        << std::endl;
    return 1;
  }
  std::string const project_id = argv[1];
  std::string const instance_id = argv[2];
  std::string const filename = argv[3];
  std::string const yyyymmdd = argv[4];

  // ... use the default credentials, this works automatically if your
  // GCE instance has the Bigtable APIs enabled.  You can also set
  // GOOGLE_APPLICATION_CREDENTIALS to work outside GCE, more details
  // at:
  //  https://grpc.io/docs/guides/auth.html
  auto creds = grpc::GoogleDefaultCredentials();

  // ... create the table if needed ...
  auto table_name = create_table(creds, project_id, instance_id, "daily");

  // ... notice that Bigtable has separate endpoints for different APIs,
  // we are going to upload some data, so the correct endpoint is:
  auto channel = grpc::CreateChannel("bigtable.googleapis.com", creds);

  std::unique_ptr<bigtable::Bigtable::Stub> bt_stub(
      bigtable::Bigtable::NewStub(channel));

  // ... we just upload the first max_lines from the input file
  // because otherwise the demo can take hours to finish uploading the
  // data.  For very large uploads the application should use
  // something like Cloud Dataflow, where the upload work is sharded
  // across many clients, and should really take advantage of the
  // batch APIs ...
  int const max_lines = 1000000;
  int const report_progress_rate = 20000;
  // ... we upload batch_size rows at a time, nothing magical about
  // 1024, just a nice round number picked by the author ...
  int const batch_size = 1024;

  std::ifstream is(filename);
  std::string line;
  std::getline(is, line, '\n');  // ... skip the header ...

  // ... the data in a TAQ file is sorted by ticker, so we can upload
  // each group of quotes back to back ...
  bigtable::MutateRowsRequest request;
  request.set_table_name(table_name);
  Quotes quotes;
  for (int lineno = 1; lineno != max_lines and not is.eof() and is; ++lineno) {
    std::getline(is, line, '\n');
    auto q = bigtable_api_samples::parse_taq_line(lineno, line);
    if (quotes.ticker() != q.ticker()) {
      if (not quotes.ticker().empty()) {
	append_to_request(request, yyyymmdd, quotes);
      }
      quotes.set_ticker(q.ticker());
      quotes.clear_timestamp_ns();
      quotes.clear_bid_px();
      quotes.clear_bid_qty();
      quotes.clear_offer_px();
      quotes.clear_offer_qty();
    }
    quotes.add_timestamp_ns(q.timestamp_ns());
    quotes.add_bid_px(q.bid_px());
    quotes.add_bid_qty(q.bid_qty());
    quotes.add_offer_px(q.offer_px());
    quotes.add_offer_qty(q.offer_qty());

    if (request.entries_size() >= batch_size) {
      mutate_with_retries(*bt_stub, request);
    }
    if (lineno % report_progress_rate == 0) {
      std::cout << lineno << " quotes uploaded so far" << std::endl;
    }
  }
  // ... CS101: the last batch needs to be uploaded too ...
  append_to_request(request, yyyymmdd, quotes);
  mutate_with_retries(*bt_stub, request);
  std::cout << max_lines << " quotes successfully uploaded" << std::endl;

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception raised: " << ex.what() << std::endl;
  return 1;
} catch (...) {
  std::cerr << "Unknown exception raised" << std::endl;
  return 1;
}

namespace {
void append_to_request(bigtable::MutateRowsRequest& request,
                       std::string const& yyyymmdd,
		       Quotes const& quotes) {
  // ... add one more entry to the batch request ...
  auto& entry = *request.add_entries();
  entry.set_row_key(yyyymmdd + "/" + quotes.ticker());
  auto& set_cell = *entry.add_mutations()->mutable_set_cell();
  set_cell.set_family_name("taq");
  set_cell.set_column_qualifier("quotes");
  std::string value;
  if (not quotes.SerializeToString(&value)) {
    std::ostringstream os;
    os << "could not serialize quotes for " << quotes.ticker();
    throw std::runtime_error(os.str());
  }
  set_cell.mutable_value()->swap(value);
  // ... we use the timestamp field as a simple revision count in
  // this example, so set it to 0.  The actual timestamp of the
  // quote is stored in the key ...
  set_cell.set_timestamp_micros(0);
}

bool should_retry(int code) {
  return (code == grpc::ABORTED or code == grpc::UNAVAILABLE or
          code == grpc::DEADLINE_EXCEEDED);
}

void mutate_with_retries(bigtable::Bigtable::Stub& bt_stub,
                         bigtable::MutateRowsRequest& request) {
  using namespace std::chrono_literals;
  // These should be parameters in a real application, but in a demon we can
  // hardcode all kinds of stuff ...
  int const max_retries = 100;
  // ... do an exponential backoff for retries ...
  auto const initial_backoff = 10ms;
  auto const maximum_backoff = 5min;

  auto backoff = initial_backoff;
  char const* retry_msg = "retrying .";
  for (int i = 0; i != max_retries; ++i) {
    // ... a variable to prepare the next retry ...
    bigtable::MutateRowsRequest tmp;
    // ... a variable to accumulate any permanent errors in this request ...
    std::ostringstream os;

    // ... MutateRows() is a streaming RPC call, make the call and read from the
    // stream ...
    grpc::ClientContext ctx;
    auto stream = bt_stub.MutateRows(&ctx, request);
    bigtable::MutateRowsResponse response;
    while (stream->Read(&response)) {
      // ... save the partial results to either `tmp` or `os` ...
      for (auto const& entry : response.entries()) {
        auto& status = entry.status();
        if (status.code() == grpc::OK) {
          continue;
        }
        if (not should_retry(status.code())) {
          // ... permanent error, save all of them for later ...
          std::string details;
          (void)google::protobuf::TextFormat::PrintToString(entry, &details);
          os << "permanent error for #" << entry.index() << ": "
             << status.message() << " [" << status.code() << "] " << details
             << "\n";
        } else {
          tmp.add_entries()->Swap(request.mutable_entries(entry.index()));
        }
      }
    }
    if (not os.str().empty()) {
      // ... there was at least one permanent error, abort ...
      throw std::runtime_error(os.str());
    }
    if (tmp.entries_size() == 0) {
      // ... nothing to retry, just return ...
      if (i > 0) {
        std::cout << " done" << std::endl;
      }
      request.mutable_entries()->Clear();
      return;
    }
    // ... prepare the next request ...
    tmp.mutable_table_name()->swap(*request.mutable_table_name());
    tmp.Swap(&request);
    backoff = backoff * 2;
    if (backoff > maximum_backoff) {
      backoff = maximum_backoff;
    }
    // ... we should randomize this sleep to avoid synchronized
    // backoffs when running multiple clients, that is beyond the
    // scope of a simple demo like this one ...
    std::this_thread::sleep_for(backoff);
    // ... give the user some sense of progress ...
    std::cout << retry_msg << std::flush;
    retry_msg = ".";
  }
  throw std::runtime_error("Could not complete mutation after maximum retries");
}

// Create a table in the given project and instance
std::string create_table(std::shared_ptr<grpc::ChannelCredentials> creds,
                         std::string const& project_id,
                         std::string const& instance_id,
                         std::string const& table_id) {
  // ... notice that Bigtable has separate endpoints for different APIs,
  // we are going to create a table, which is one of the admin APIs,
  // so connect to the bigtableadmin endpoint ...
  auto admin_channel =
      grpc::CreateChannel("bigtableadmin.googleapis.com", creds);

  // ... save ourselves some typing ...
  namespace admin = ::google::bigtable::admin::v2;
  using admin::BigtableTableAdmin;
  std::unique_ptr<BigtableTableAdmin::Stub> table_admin(
      BigtableTableAdmin::NewStub(admin_channel));
  admin::CreateTableRequest request;
  request.set_parent(std::string("projects/") + project_id + "/instances/" +
                     instance_id);
  request.set_table_id(table_id);

  // ... in this demo we create a table with two column families
  // "quotes" and "trades", vaguely movivated by financial markets
  // data ...
  auto& taq = (*request.mutable_table()->mutable_column_families())["taq"];
  taq.mutable_gc_rule()->set_max_num_versions(1);

  auto& summary = (*request.mutable_table()->mutable_column_families())["summary"];
  summary.mutable_gc_rule()->set_max_num_versions(1);

  grpc::ClientContext context;
  admin::Table response;
  auto status = table_admin->CreateTable(&context, request, &response);
  if (status.error_code() == grpc::ALREADY_EXISTS) {
    return std::string("projects/") + project_id + "/instances/" +
      instance_id + "/tables/" + table_id;
  }
  if (not status.ok()) {
    std::ostringstream os;
    os << "Error in CreateTable() request: " << status.error_message()
       << " [" << status.error_code() << "] " << status.error_details();
    throw std::runtime_error(os.str());
  }

  std::string text;
  (void)google::protobuf::TextFormat::PrintToString(response, &text);
  std::cout << "CreateTable() operation was successful with result=" << text
            << std::endl;
  return response.name();
}
}  // anonymous namespace
