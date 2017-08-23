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

#ifndef raw_taq_uploader_h
#define raw_taq_uploader_h

#include "client.h"

#include <ciso646>
#include <condition_variable>
#include <chrono>
#include <deque>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

namespace bigtable_api_samples {
/**
 * Upload Quote or Trade messages to a raw table.
 *
 * @tparam Message the type of message (a protobuf) to upload.
 */
template <typename Message>
class raw_taq_uploader {
 public:
  raw_taq_uploader(std::shared_ptr<grpc::ChannelCredentials> credentials,
                   std::string const& table_name, std::string const& yyyymmdd,
                   int report_progress_rate, int batch_size)
      : mu_(),
        cv_(),
        queue_(),
        is_shutdown_(false),
        client_(credentials),
        table_name_(table_name),
        yyyymmdd_(yyyymmdd),
        report_progress_rate_(report_progress_rate),
        batch_size_(batch_size) {}

  /// New message to upload
  void push(Message msg) {
    std::unique_lock<std::mutex> lock(mu_);
    bool was_empty = queue_.empty();
    queue_.emplace_back(std::move(msg));
    if (was_empty) {
      lock.unlock();
      cv_.notify_all();
    }
  }

  /// Called to indicate there is no more data to upload.
  void shutdown() {
    std::unique_lock<std::mutex> lock(mu_);
    is_shutdown_ = true;
    lock.unlock();
    cv_.notify_all();
  }

  /**
   * Run the event loop to upload messages.
   *
   * Can be called from multiple threads to increase parallelism.
   */
  void run() {
    google::bigtable::v2::MutateRowsRequest request;
    request.set_table_name(table_name_);
    using namespace std::chrono;
    auto upload_start = steady_clock::now();
    int count = 0;

    std::cout << "[" << std::this_thread::get_id() << "] runnning ..."
              << std::endl;

    while (true) {
      // ... sigh, C++17 makes this much nicer: auto [t, empty] =
      // wait_for_message();
      auto result = wait_for_message();
      if (result.second) {
        // ... it is shutdown and queue was empty ...
        break;
      }
      auto const& t = result.first;
      ++count;
      auto& entry = *request.add_entries();
      entry.set_row_key(t.ticker() + "/" + yyyymmdd_ + "/" +
                        std::to_string(t.timestamp_ns()));
      auto& set_cell = *entry.add_mutations()->mutable_set_cell();
      set_cell.set_family_name("taq");
      set_cell.set_column_qualifier("event");
      std::string value;
      if (not t.SerializeToString(&value)) {
        std::ostringstream os;
        os << "could not serialize message for " << t.ticker() << " "
           << t.timestamp_ns();
        throw std::runtime_error(os.str());
      }
      set_cell.mutable_value()->swap(value);
      // ... we use the timestamp field as a simple revision count in
      // this example, so set it to 0.  The actual timestamp of the
      // quote is stored in the key ...
      set_cell.set_timestamp_micros(0);

      if (request.entries_size() >= batch_size_) {
        client_.mutate_rows(std::move(request), backoff_config());
        request.set_table_name(table_name_);
      }
      if (count % report_progress_rate_ == 0) {
        auto elapsed = steady_clock::now() - upload_start;
        std::cout << "[" << std::this_thread::get_id() << "] " << count
                  << " messages uploaded. elapsed-time="
                  << duration_cast<seconds>(elapsed).count() << "s"
                  << std::endl;
      }
    }
    // ... CS101: the last batch needs to be uploaded too ...
    client_.mutate_rows(std::move(request), backoff_config());
    auto elapsed = steady_clock::now() - upload_start;
    std::cout << "[" << std::this_thread::get_id() << "] " << count
              << " messages uploaded at shutdown. elapsed-time="
              << duration_cast<seconds>(elapsed).count() << "s" << std::endl;
  }

 private:
  /// Block until a new message is available.
  std::pair<Message, bool> wait_for_message() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this]() { return is_shutdown_ or not queue_.empty(); });
    if (queue_.empty()) {
      return std::make_pair(Message(), true);
    }
    Message msg;
    msg.Swap(&queue_.front());
    queue_.pop_front();
    return std::make_pair(std::move(msg), false);
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Message> queue_;
  bool is_shutdown_;
  client client_;
  std::string table_name_;
  std::string yyyymmdd_;
  int report_progress_rate_;
  int batch_size_;
};
}  // namespace bigtable_api_samples

#endif  // raw_taq_uploader_h
