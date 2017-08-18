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

#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/protobuf/text_format.h>
#include <google/rpc/status.pb.h>
#include <grpc++/grpc++.h>

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
        credentials_(credentials),
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
    // ... notice that Bigtable has separate endpoints for different APIs,
    // we are going to upload some data, so the correct endpoint is:
    auto channel = grpc::CreateChannel("bigtable.googleapis.com", credentials_);

    std::unique_ptr<google::bigtable::v2::Bigtable::Stub> bt_stub(
        google::bigtable::v2::Bigtable::NewStub(channel));

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
        mutate_with_retries(*bt_stub, request);
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
    mutate_with_retries(*bt_stub, request);
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

  /// Return true if the grpc::StatusCode can be retried
  bool should_retry(int code) {
    return (code == grpc::ABORTED or code == grpc::UNAVAILABLE or
            code == grpc::DEADLINE_EXCEEDED);
  }

  // Perform a Bigtable::MutateRows() request until all mutations
  // complete.
  void mutate_with_retries(google::bigtable::v2::Bigtable::Stub& bt_stub,
                           google::bigtable::v2::MutateRowsRequest& request) {
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
      google::bigtable::v2::MutateRowsRequest tmp;
      // ... a variable to accumulate any permanent errors in this request ...
      std::ostringstream os;

      // ... MutateRows() is a streaming RPC call, make the call and read from
      // the
      // stream ...
      grpc::ClientContext ctx;
      auto stream = bt_stub.MutateRows(&ctx, request);
      google::bigtable::v2::MutateRowsResponse response;
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
    throw std::runtime_error(
        "Could not complete mutation after maximum retries");
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Message> queue_;
  bool is_shutdown_;
  std::shared_ptr<grpc::ChannelCredentials> credentials_;
  std::string table_name_;
  std::string yyyymmdd_;
  int report_progress_rate_;
  int batch_size_;
};
}  // namespace bigtable_api_samples

#endif  // raw_taq_uploader_h
