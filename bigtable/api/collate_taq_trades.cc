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

#include "taq.pb.h"

#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/protobuf/text_format.h>
#include <google/rpc/status.pb.h>
#include <grpc++/grpc++.h>

#include <ciso646>
#include <sstream>
#include <thread>

namespace {
// ... save ourselves some typing ...
namespace bigtable = ::google::bigtable::v2;

void collate_trades(std::shared_ptr<grpc::ChannelCredentials> credentials,
                    std::string const& table_prefix,
                    std::string const& yyyymmdd, int report_progress_rate,
                    int batch_size);
}  // anonymous namespace

// Demonstrate how to read and write from Bigtable in a single
// application.  This example takes the data stored by the
// simulate_taq_capture demo and collates the data by symbol, which
// makes it much easier to operate on.  The read_rows demo shows how
// to use the collated data.
int main(int argc, char* argv[]) try {
  // ... a more interesting application would use getopt(3),
  // getopt_long(3), or Boost.Options to parse the command-line, we
  // want to keep things simple in the example ...
  if (argc != 4) {
    std::cerr << "Usage: read_rows <project_id> <instance_id> <date>"
              << std::endl;
    return 1;
  }
  std::string const project_id = argv[1];
  std::string const instance_id = argv[2];
  std::string const yyyymmdd = argv[3];

  // ... every few lines print out the progress because the author is
  // impatient ...
  int const report_progress_rate = 10000;
  // ... we upload batch_size rows at a time, nothing magical about
  // 1024, just a nice round number picked by the author ...
  int const batch_size = 128;

  // ... use the default credentials, this works automatically if your
  // GCE instance has the Bigtable APIs enabled.  You can also set
  // GOOGLE_APPLICATION_CREDENTIALS to work outside GCE, more details
  // at:
  //  https://grpc.io/docs/guides/auth.html
  auto credentials = grpc::GoogleDefaultCredentials();

  // ... create the table if needed ...
  std::string table_prefix =
      "projects/" + project_id + "/instances/" + instance_id + "/tables/";

  std::thread trades([=]() {
      collate_trades(credentials, table_prefix, yyyymmdd, report_progress_rate,
		     batch_size);
    });

  trades.join();

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception raised: " << ex.what() << std::endl;
  return 1;
} catch (...) {
  std::cerr << "Unknown exception raised" << std::endl;
  return 1;
}

namespace {
// Append a set of quotes to a mutate request
template <typename Message>
void append_to_request(bigtable::MutateRowsRequest& request,
                       std::string const& yyyymmdd,
		       char const* column,
		       Message const& msg) {
  // ... add one more entry to the batch request ...
  auto& entry = *request.add_entries();
  entry.set_row_key(yyyymmdd + "/" + msg.ticker());
  auto& set_cell = *entry.add_mutations()->mutable_set_cell();
  set_cell.set_family_name("taq");
  set_cell.set_column_qualifier(column);
  std::string value;
  if (not msg.SerializeToString(&value)) {
    std::ostringstream os;
    os << "could not serialize " << column << " for " << msg.ticker();
    throw std::runtime_error(os.str());
  }
  set_cell.mutable_value()->swap(value);
  // ... we use the timestamp field as a simple revision count in
  // this example, so set it to 0.  The actual timestamp of the
  // quote is stored in the key ...
  set_cell.set_timestamp_micros(0);
}

// Perform a Bigtable::MutateRows() request until all mutations complete.
void mutate_with_retries(bigtable::Bigtable::Stub& bt_stub,
                         bigtable::MutateRowsRequest& req);

void collate_trades(std::shared_ptr<grpc::ChannelCredentials> credentials,
                    std::string const& table_prefix,
                    std::string const& yyyymmdd, int report_progress_rate,
                    int batch_size) {
  // ... notice that Bigtable has separate endpoints for different APIs,
  // we are going to upload some data, so the correct endpoint is:
  auto channel = grpc::CreateChannel("bigtable.googleapis.com", credentials);

  std::unique_ptr<bigtable::Bigtable::Stub> bt_stub(
      bigtable::Bigtable::NewStub(channel));

  // ... prepare the request to update the destination table ...
  bigtable::MutateRowsRequest mutation;
  mutation.set_table_name(table_prefix + "daily");
  Trades trades;
  using namespace std::chrono;
  auto upload_start = steady_clock::now();
  int count = 0;
  int chunk_count = 0;

  // ... once the table is created and the data uploaded we can make
  // the query ...
  bigtable::ReadRowsRequest request;
  request.set_table_name(table_prefix + "raw-trades");
  auto& chain = *request.mutable_filter()->mutable_chain();
  // ... first only accept the "taq" column family ...
  chain.add_filters()->set_family_name_regex_filter("taq");
  // ... then only accept the "trades" column ...
  chain.add_filters()->set_column_qualifier_regex_filter("trade");

  grpc::ClientContext context;
  auto stream = bt_stub->ReadRows(&context, request);

  std::string current_row_key;
  std::string current_column_family;
  std::string current_column;
  std::string current_value;
  // ... receive the streaming response ...
  bigtable::ReadRowsResponse response;
  while (stream->Read(&response)) {
    for (auto& cell_chunk : *response.mutable_chunks()) {
      ++chunk_count;
      if (not cell_chunk.row_key().empty()) {
        current_row_key = cell_chunk.row_key();
      }
      if (cell_chunk.has_family_name()) {
        current_column_family = cell_chunk.family_name().value();
        if (current_column_family != "taq") {
          throw std::runtime_error(
              "strange, only 'taq' family name expected in the query");
        }
      }
      if (cell_chunk.has_qualifier()) {
        current_column = cell_chunk.qualifier().value();
        if (current_column != "trade") {
          throw std::runtime_error(
              "strange, only 'trade' column expected in the query");
        }
      }
      if (cell_chunk.timestamp_micros() != 0) {
        throw std::runtime_error(
            "strange, only the 0 timestamp expected in the query");
      }
      if (cell_chunk.value_size() > 0) {
        current_value.reserve(cell_chunk.value_size());
      }
      current_value.append(cell_chunk.value());
      if (cell_chunk.commit_row()) {
	++count;
        // ... process this cell, we want to convert the value into a Trades
        // proto ...
        Trade q;
        if (not q.ParseFromString(current_value)) {
          std::cerr << current_row_key << ": ParseFromString() failed"
                    << std::endl;
          continue;
        }
        if (trades.ticker() != q.ticker()) {
          if (not trades.ticker().empty()) {
            append_to_request(mutation, yyyymmdd, "trades", trades);
	    if (mutation.entries_size() >= batch_size) {
	      mutate_with_retries(*bt_stub, mutation);
	    }
          }
          trades.set_ticker(q.ticker());
          trades.clear_timestamp_ns();
          trades.clear_px();
          trades.clear_qty();
          trades.clear_sale_condition();
        }
        trades.add_timestamp_ns(q.timestamp_ns());
        trades.add_px(q.px());
        trades.add_qty(q.qty());
        trades.add_sale_condition(q.sale_condition());

        if (count % report_progress_rate == 0) {
          auto elapsed = steady_clock::now() - upload_start;
          std::cout << count << " trades in " << chunk_count << " chunks.  elapsed-time="
                    << duration_cast<seconds>(elapsed).count() << "s" << std::endl;
        }
      }
    }
  }
  // ... CS101: the last batch needs to be uploaded too ...
  append_to_request(mutation, yyyymmdd, "trades", trades);
  mutate_with_retries(*bt_stub, mutation);

  auto elapsed = steady_clock::now() - upload_start;
  std::cout << count << " trades in " << chunk_count << " chunks.  elapsed-time="
	    << duration_cast<seconds>(elapsed).count() << "s" << std::endl;
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
}  // anonymous namespace
