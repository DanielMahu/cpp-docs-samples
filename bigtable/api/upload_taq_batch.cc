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

#include "client.h"
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
  if (argc != 6) {
    std::cerr
        << "Usage: create_table <project_id> <instance_id> <table> <yyyymmdd> <filename>"
        << std::endl;
   return 1;
  }
  std::string const project_id = argv[1];
  std::string const instance_id = argv[2];
  std::string const table_id = argv[3];
  std::string const yyyymmdd = argv[4];
  std::string const filename = argv[5];

  auto creds = grpc::GoogleDefaultCredentials();
  bigtable_api_samples::client client(creds);

  std::string table_name = std::string("projects/") + project_id +
                           "/instances/" + instance_id + "/tables/" + table_id;

  // ... we just upload the first max_lines from the input file
  // because otherwise the demo can take hours to finish uploading the
  // data.  For very large uploads the application should use
  // something like Cloud Dataflow, where the upload work is sharded
  // across many clients, and should really take advantage of the
  // batch APIs ...
  int const max_lines_to_upload = 1000000;
  // ... every few lines print out the progress because the author is
  // impatient ...
  int const report_progress_rate = 20000;
  // ... we upload batch_size rows at a time, nothing magical about
  // 1024, just a nice round number picked by the author ...
  int const batch_size = 1024;

  std::ifstream is(filename);
  std::string line;
  // ... skip the header line in the file ...
  std::getline(is, line, '\n');

  bigtable::MutateRowsRequest request;
  request.set_table_name(table_name);
  Quotes quotes;
  int lineno = 1;
  for (; lineno != max_lines_to_upload and not is.eof() and is; ++lineno) {
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
      client.mutate_rows(std::move(request), bigtable_api_samples::backoff_config());
      request.set_table_name(table_name);
    }
    if (lineno % report_progress_rate == 0) {
      std::cout << lineno << " quotes uploaded so far" << std::endl;
    }
  }
  // ... CS101: the last batch needs to be uploaded too ...
  append_to_request(request, yyyymmdd, quotes);
  client.mutate_rows(std::move(request), bigtable_api_samples::backoff_config());
  std::cout << lineno << " quotes successfully uploaded" << std::endl;

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
}  // anonymous namespace
