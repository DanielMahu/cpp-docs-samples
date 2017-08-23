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

/**
 * @file
 *
 * Upload data for read-write example.  We want to build up to an
 * example that reads and writes data from Bigtable in a single
 * application (that is collate_taq.cc).  In this program we prepare
 * two tables, one with trade data where each row contains a single
 * trade event.  A second table contains quote data where each row
 * contains a single quote event.  In the collate_taq.cc program we
 * read those tables to create a single table where each row contains
 * the full timeseries of trade and quotes for a single ticker.
 *
 * This particular program is not very interesting in terms of
 * learning about the bigtable APIs.  It just prepares the tables to
 * use in the collate_taq.cc program.
 *
 * While the main purpose of this example is to prepare the data, it
 * is inspired by real world applications where market data is first
 * captured in real-time with a single row per event, and later it is
 * collated or summarized in many different ways by separate programs.
 *
 * This program just concerns itself with simulating the data
 * capture.  Instead of the usual UDP protocols use for market data we
 * simply read from a TAQ file.  Please see parse_taq_line.h for
 * details of the TAQ file format.
 */

#include "parse_taq_line.h"
#include "raw_taq_uploader.h"

#include <fstream>
#include <future>

namespace {
// ... save ourselves some typing ...
namespace bigtable = ::google::bigtable::v2;

void read_quotes_file(std::string const& filename,
                      bigtable_api_samples::raw_taq_uploader<Quote>& uploader);
void read_trades_file(std::string const& filename,
                      bigtable_api_samples::raw_taq_uploader<Trade>& uploader);
}  // anonymous namespace

int main(int argc, char* argv[]) try {
  // ... a more interesting application would use getopt(3),
  // getopt_long(3), or Boost.Options to parse the command-line, we
  // want to keep things simple in the example ...
  if (argc != 6) {
    std::cerr << "Usage: simulate_taq_capture <project_id> <instance_id> "
                 "<yyyymmdd> <quotes-file> <trades-file>"
              << std::endl;
    return 1;
  }
  std::string const project_id = argv[1];
  std::string const instance_id = argv[2];
  std::string const yyyymmdd = argv[3];
  std::string const quotes_filename = argv[4];
  std::string const trades_filename = argv[5];

  // ... every few lines print out the progress because the author is
  // impatient ...
  int const report_progress_rate = 500000;
  // ... we upload batch_size rows at a time, nothing magical about
  // 1024, just a nice round number picked by the author ...
  int const batch_size = 1024;

  namespace bas = bigtable_api_samples;
  std::string table_prefix = std::string("projects/") + project_id +
                             "/instances/" + instance_id + "/tables/";
  bas::raw_taq_uploader<Trade> trade_uploader(
      grpc::GoogleDefaultCredentials(), table_prefix + "raw-trades", yyyymmdd,
      report_progress_rate, batch_size);
  bas::raw_taq_uploader<Quote> quote_uploader(
      grpc::GoogleDefaultCredentials(), table_prefix + "raw-quotes", yyyymmdd,
      report_progress_rate, batch_size);

  // ... this example uses multiple threads because the author did not
  // have the patience to wait for a single thread to finish.  Nothing
  // is particularly interested in the use of multiple threads here ...
  int const quote_threads = 1;
  int const trade_threads = 1;

  std::map<std::string, std::future<void>> ops;
  for (int i = 0; i != trade_threads; ++i) {
    std::ostringstream os;
    os << "upload-trades[" << i << "]";
    auto future = std::async(std::launch::async,
                             [&trade_uploader]() { trade_uploader.run(); });
    ops.emplace(os.str(), std::move(future));
  }
  for (int i = 0; i != quote_threads; ++i) {
    std::ostringstream os;
    os << "upload-quotes[" << i << "]";
    auto future = std::async(std::launch::async,
                             [&quote_uploader]() { quote_uploader.run(); });
    ops.emplace(os.str(), std::move(future));
  }

  ops.emplace(
      "read-trades",
      std::async(std::launch::async, [trades_filename, &trade_uploader]() {
        read_trades_file(trades_filename, trade_uploader);
      }));

  ops.emplace(
      "read-quotes",
      std::async(std::launch::async, [quotes_filename, &quote_uploader]() {
        read_quotes_file(quotes_filename, quote_uploader);
      }));

  // ... wait for the operations to complete and report any exceptions ...
  for (auto& i : ops) {
    try {
      i.second.get();
      std::cout << i.first << " completed successfully" << std::endl;
    } catch (std::exception const& ex) {
      std::cerr << "standard exception raised by " << i.first << ": "
                << ex.what() << std::endl;
    } catch (...) {
      std::cerr << "unknown exception raised by " << i.first << std::endl;
    }
  }

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception raised: " << ex.what() << std::endl;
  return 1;
} catch (...) {
  std::cerr << "Unknown exception raised" << std::endl;
  return 1;
}

namespace {
void read_quotes_file(std::string const& filename,
                      bigtable_api_samples::raw_taq_uploader<Quote>& uploader) {
  std::cout << "Reading quotes from " << filename << std::endl;
  std::ifstream is(filename);
  std::string line;
  // ... skip the header line in the file ...
  std::getline(is, line, '\n');

  int lineno = 1;
  for (; not is.eof() and is; ++lineno) {
    std::getline(is, line, '\n');
    if (line.empty() or line.substr(0, 4) == "END|") {
      break;
    }
    auto msg = bigtable_api_samples::parse_taq_quote(lineno, line);
    uploader.push(std::move(msg));
  }
  uploader.shutdown();
  std::cout << "Finished reading quotes from " << filename << std::endl;
}

void read_trades_file(std::string const& filename,
                      bigtable_api_samples::raw_taq_uploader<Trade>& uploader) {
  std::cout << "Reading trades from " << filename << std::endl;
  std::ifstream is(filename);
  std::string line;
  // ... skip the header line in the file ...
  std::getline(is, line, '\n');

  int lineno = 1;
  for (; not is.eof() and is; ++lineno) {
    std::getline(is, line, '\n');
    if (line.empty() or line.substr(0, 4) == "END|") {
      break;
    }
    auto msg = bigtable_api_samples::parse_taq_trade(lineno, line);
    uploader.push(std::move(msg));
  }
  uploader.shutdown();
  std::cout << "Finished reading trades from " << filename << std::endl;
}

}  // anonymous namespace
