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
#include <google/protobuf/text_format.h>
#include <google/rpc/status.pb.h>
#include <grpc++/grpc++.h>

#include <algorithm>
#include <chrono>
#include <ciso646>
#include <deque>
#include <fstream>
#include <sstream>
#include <thread>

namespace {
// ... save ourselves some typing ...
namespace bigtable = ::google::bigtable::v2;

class trades_uploader {
public:
  trades_uploader(std::shared_ptr<grpc::ChannelCredentials> credentials,
		  std::string const& table_name, std::string const& yyyymmdd,
		  int report_progress_rate, int batch_size);

  void run();
  void shutdown();
  void push(Trade trade);

private:
  std::pair<Trade, bool> wait_for_message();

private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Trade> queue_;
  bool is_shutdown_;
  std::shared_ptr<grpc::ChannelCredentials> credentials_;
  std::string table_name_;
  std::string yyyymmdd_;
  int report_progress_rate_;
  int batch_size_;
};

}  // anonymous namespace

// Upload data for read-write example.  We want to build up to an
// example that reads and writes data from Bigtable in a single
// application.  First we prepare a table of trades data where each
// row represents a different trade.  In a second program
// (collate_taq) we collate this data by symbol, populating a second
// table where each row represents all the trades for a given symbol.
//
// The example is a little contrived, but not too much, market data is
// often captured in real-time with a single row per event, and later
// it is collated in many different ways.
//
// This program just concerns itself with simulating the data
// capture.  Instead of the usual UDP protocols use for market data we
// simply read from a TAQ file.  Please see parse_taq_line.h for
// details of the file format.
//
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

  std::string table_prefix = std::string("projects/") + project_id +
                             "/instances/" + instance_id + "/tables/";

  // ... every few lines print out the progress because the author is
  // impatient ...
  int const report_progress_rate = 100000;
  // ... we upload batch_size rows at a time, nothing magical about
  // 1024, just a nice round number picked by the author ...
  int const batch_size = 4096;

  trades_uploader tu(grpc::GoogleDefaultCredentials(),
		     table_prefix + "raw-trades", yyyymmdd,
		     report_progress_rate, batch_size);

  // ... run 8 threads for the upload, it is painfully slow otherwise ...
  int constexpr pool_size = 8;
  std::thread trades_pool[pool_size];
  for (int i = 0; i != pool_size; ++i) {
    trades_pool[i] = std::move(std::thread([&tu]() { tu.run(); }));
  }

  std::ifstream is(trades_filename);
  std::string line;
  // ... skip the header line in the file ...
  std::getline(is, line, '\n');

  int lineno = 1;
  for (; not is.eof() and is; ++lineno) {
    std::getline(is, line, '\n');
    if (line.empty() or line.substr(0, 4) == "END|") {
      break;
    }
    auto t = bigtable_api_samples::parse_taq_trade(lineno, line);
    tu.push(std::move(t));
  }
  tu.shutdown();
  for (int i = 0; i != pool_size; ++i) {
    trades_pool[i].join();
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
bool should_retry(int code) {
  return (code == grpc::ABORTED or code == grpc::UNAVAILABLE or
          code == grpc::DEADLINE_EXCEEDED);
}

// Perform a Bigtable::MutateRows() request until all mutations complete.
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


trades_uploader::trades_uploader(std::shared_ptr<grpc::ChannelCredentials> credentials,
				 std::string const& table_name, std::string const& yyyymmdd,
				 int report_progress_rate, int batch_size)
  : mu_()
  , cv_()
  , queue_()
  , is_shutdown_(false)
  , credentials_(credentials)
  , table_name_(table_name)
  , yyyymmdd_(yyyymmdd)
  , report_progress_rate_(report_progress_rate)
  , batch_size_(batch_size) {
}

void trades_uploader::run() {
  // ... notice that Bigtable has separate endpoints for different APIs,
  // we are going to upload some data, so the correct endpoint is:
  auto channel = grpc::CreateChannel("bigtable.googleapis.com", credentials_);

  std::unique_ptr<bigtable::Bigtable::Stub> bt_stub(
      bigtable::Bigtable::NewStub(channel));

  bigtable::MutateRowsRequest request;
  request.set_table_name(table_name_);
  using namespace std::chrono;
  auto upload_start = steady_clock::now();
  int count = 0;

  std::cout << "[" << std::this_thread::get_id() << "] runnning ..." << std::endl;

  while (true) {
    // ... sigh, C++17 makes this much nicer: auto [t, empty] = wait_for_message();
    auto result = wait_for_message();
    if (result.second) {
      // ... it is shutdown and queue was empty ...
      break;
    }
    auto const& t = result.first;
    ++count;
    auto& entry = *request.add_entries();
    entry.set_row_key(t.ticker() + "/" + yyyymmdd_ + "/" + std::to_string(t.timestamp_ns()));
    auto& set_cell = *entry.add_mutations()->mutable_set_cell();
    set_cell.set_family_name("taq");
    set_cell.set_column_qualifier("trade");
    std::string value;
    if (not t.SerializeToString(&value)) {
      std::ostringstream os;
      os << "could not serialize trade for " << t.ticker() << " " << t.timestamp_ns();
      throw std::runtime_error(os.str());
    }
    set_cell.mutable_value()->swap(value);
    // ... we use the timestamp field as a simple revision count in
    // this example, so set it to 0.  The actual timestamp of the
    // quote is stored in the key ...
    set_cell.set_timestamp_micros(0);

    if (request.entries_size() >= batch_size_) {
      mutate_with_retries(*bt_stub, request);
    }
    if (count % report_progress_rate_ == 0) {
      auto elapsed = steady_clock::now() - upload_start;
      std::cout << "[" << std::this_thread::get_id() << "] " << count << " messages uploaded. elapsed-time="
		<< duration_cast<seconds>(elapsed).count() << "s" << std::endl;
    }
  }
  // ... CS101: the last batch needs to be uploaded too ...
  mutate_with_retries(*bt_stub, request);
  auto elapsed = steady_clock::now() - upload_start;
  std::cout << "[" << std::this_thread::get_id() << "] " << count << " messages uploaded at shutdown. elapsed-time="
	    << duration_cast<seconds>(elapsed).count() << "s" << std::endl;
}

void trades_uploader::shutdown() {
  std::unique_lock<std::mutex> lock(mu_);
  is_shutdown_ = true;
  lock.unlock();
  cv_.notify_all();
}

void trades_uploader::push(Trade trade) {
  std::unique_lock<std::mutex> lock(mu_);
  bool was_empty = queue_.empty();
  queue_.emplace_back(std::move(trade));
  if (was_empty) {
    lock.unlock();
    cv_.notify_all();
  }
}

std::pair<Trade, bool> trades_uploader::wait_for_message() {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this]() { return is_shutdown_ or not queue_.empty(); });
  if (queue_.empty()) {
    return std::make_pair(Trade(), true);
  }
  Trade t;
  t.Swap(&queue_.front());
  queue_.pop_front();
  return std::make_pair(std::move(t), false);
}

}  // anonymous namespace
