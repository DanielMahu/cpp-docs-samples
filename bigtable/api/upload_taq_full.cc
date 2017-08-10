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

#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/rpc/status.pb.h>
#include <google/protobuf/text_format.h>
#include <grpc++/grpc++.h>

#include <algorithm>
#include <chrono>
#include <ciso646>
#include <fstream>
#include <sstream>
#include <thread>

namespace {
// ... save ourselves some typing ...
namespace bigtable = ::google::bigtable::v2;

// Append a set of quotes to a mutate request
void append_to_request(bigtable::MutateRowsRequest& request,
                       std::string const& yyyymmdd,
		       Quotes const& quotes);

// Perform a Bigtable::MutateRows() request until all mutations complete.
void mutate_with_retries(bigtable::Bigtable::Stub& bt_stub,
			 bigtable::MutateRowsRequest& req);

// Upload a file of TAQ quotes to a given table
void upload_quotes(std::string const& table_name,
		   std::string const& yyyymmdd,
		   std::string const& filename,
		   int report_progress_rate,
		   int batch_size);

// Upload a file of TAQ trades to a given table
void upload_trades(std::string const& table_name,
		   std::string const& yyyymmdd,
		   std::string const& filename,
		   int report_progress_rate,
		   int batch_size);

}  // anonymous namespace

// We want to show a more efficient way to update rows in Bigtable,
// batching multiple updates into a single request.
//
// We use TAQ data for the source, this is a text file, with fields
// separated by '|' characters (so really a CSV file with an uncommon
// separator), with contents like this:
//
// timestamp|exchange|ticker|bid price|bid qty|offer price|offer qty|...
// 093000123456789|K|GOOG|800.00|100|900.00|200|...
// 093001123456789|K|GOOG|801.00|200|901.00|300|...
//
// Each row represents a market data quote, each file represents a
// different day of trading, so the timestamps are expressed in
// nanoseconds since midnight (in a strange format, but we digress).
//
// In this example we will collect all the quotes for a single symbol
// and upload them to a single row and cell in a Bigtable.
int main(int argc, char* argv[]) try {
  // ... a more interesting application would use getopt(3),
  // getopt_long(3), or Boost.Options to parse the command-line, we
  // want to keep things simple in the example ...
  if (argc != 7) {
    std::cerr
        << "Usage: create_table <project_id> <instance_id> <table> <yyyymmdd> <quotes-file> <trades-file>"
        << std::endl;
   return 1;
  }
  std::string const project_id = argv[1];
  std::string const instance_id = argv[2];
  std::string const table_id = argv[3];
  std::string const yyyymmdd = argv[4];
  std::string const quotes_filename = argv[5];
  std::string const trades_filename = argv[6];

  std::string table_name = std::string("projects/") + project_id +
                           "/instances/" + instance_id + "/tables/" + table_id;

  // ... every few lines print out the progress because the author is
  // impatient ...
  int const report_progress_rate = 50000;
  // ... we upload batch_size rows at a time, nothing magical about
  // 1024, just a nice round number picked by the author ...
  int const batch_size = 1024;

  upload_quotes(table_name, yyyymmdd, quotes_filename, report_progress_rate, batch_size);
  upload_trades(table_name, yyyymmdd, quotes_filename, report_progress_rate, batch_size);

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

void append_to_request(bigtable::MutateRowsRequest& request,
                       std::string const& yyyymmdd,
		       Trades const& trades) {
  // ... add one more entry to the batch request ...
  auto& entry = *request.add_entries();
  entry.set_row_key(yyyymmdd + "/" + trades.ticker());
  auto& set_cell = *entry.add_mutations()->mutable_set_cell();
  set_cell.set_family_name("taq");
  set_cell.set_column_qualifier("trades");
  std::string value;
  if (not trades.SerializeToString(&value)) {
    std::ostringstream os;
    os << "could not serialize quotes for " << trades.ticker();
    throw std::runtime_error(os.str());
  }
  set_cell.mutable_value()->swap(value);
  // ... we use the timestamp field as a simple revision count in
  // this example, so set it to 0.  The actual timestamp of the
  // quote is stored in the key ...
  set_cell.set_timestamp_micros(0);
}

bool should_retry(int code) {
  return (code == grpc::ABORTED or code == grpc::UNAVAILABLE
	  or code == grpc::DEADLINE_EXCEEDED);
}

void mutate_with_retries(bigtable::Bigtable::Stub& bt_stub,
			 bigtable::MutateRowsRequest& request) {
  using namespace std::chrono_literals;
  // These should be parameters in a real application, but in a demon we can hardcode all kinds of stuff ...
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

    // ... MutateRows() is a streaming RPC call, make the call and read from the stream ...
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
	  os << "permanent error for #" << entry.index()
	     << ": " << status.message() << " [" << status.code() << "] "
	     << details << "\n";
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

void upload_quotes(std::string const& table_name,
		   std::string const& yyyymmdd,
		   std::string const& filename,
		   int report_progress_rate,
		   int batch_size) {
  auto creds = grpc::GoogleDefaultCredentials();
  // ... notice that Bigtable has separate endpoints for different APIs,
  // we are going to upload some data, so the correct endpoint is:
  auto channel = grpc::CreateChannel("bigtable.googleapis.com", creds);

  std::unique_ptr<bigtable::Bigtable::Stub> bt_stub(
      bigtable::Bigtable::NewStub(channel));

  std::ifstream is(filename);
  std::string line;
  // ... skip the header line in the file ...
  std::getline(is, line, '\n');

  bigtable::MutateRowsRequest request;
  request.set_table_name(table_name);
  Quotes quotes;
  int lineno = 1;
  for (; not is.eof() and is; ++lineno) {
    std::getline(is, line, '\n');
    auto q = bigtable_api_samples::parse_taq_quote(lineno, line);
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
  std::cout << lineno << " quotes successfully uploaded" << std::endl;
}

void upload_trades(std::string const& table_name,
		   std::string const& yyyymmdd,
		   std::string const& filename,
		   int report_progress_rate,
		   int batch_size) {
  auto creds = grpc::GoogleDefaultCredentials();
  // ... notice that Bigtable has separate endpoints for different APIs,
  // we are going to upload some data, so the correct endpoint is:
  auto channel = grpc::CreateChannel("bigtable.googleapis.com", creds);

  std::unique_ptr<bigtable::Bigtable::Stub> bt_stub(
      bigtable::Bigtable::NewStub(channel));

  std::ifstream is(filename);
  std::string line;
  // ... skip the header line in the file ...
  std::getline(is, line, '\n');

  bigtable::MutateRowsRequest request;
  request.set_table_name(table_name);
  Trades trades;
  int lineno = 1;
  for (; not is.eof() and is; ++lineno) {
    std::getline(is, line, '\n');
    auto t = bigtable_api_samples::parse_taq_trade(lineno, line);
    if (trades.ticker() != t.ticker()) {
      if (not trades.ticker().empty()) {
	append_to_request(request, yyyymmdd, trades);
      }
      trades.set_ticker(t.ticker());
      trades.clear_timestamp_ns();
      trades.clear_px();
      trades.clear_qty();
      trades.clear_sale_condition();
    }
    trades.add_timestamp_ns(t.timestamp_ns());
    trades.add_px(t.px());
    trades.add_qty(t.qty());
    trades.add_sale_condition(t.sale_condition());

    if (request.entries_size() >= batch_size) {
      mutate_with_retries(*bt_stub, request);
    }
    if (lineno % report_progress_rate == 0) {
      std::cout << lineno << " quotes uploaded so far" << std::endl;
    }
  }
  // ... CS101: the last batch needs to be uploaded too ...
  append_to_request(request, yyyymmdd, trades);
  mutate_with_retries(*bt_stub, request);
  std::cout << lineno << " trades successfully uploaded" << std::endl;
}

}  // anonymous namespace
