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

#include <google/protobuf/text_format.h>
#include <google/rpc/status.pb.h>

#include <ciso646>
#include <iostream>
#include <sstream>
#include <thread>

namespace {
bool should_retry(int code) {
  return (code == grpc::ABORTED or code == grpc::UNAVAILABLE or
          code == grpc::DEADLINE_EXCEEDED);
}
} // anonymous namespace

namespace bigtable_api_samples {

void client::mutate_rows(google::bigtable::v2::MutateRowsRequest&& r,
                         backoff_config const& boc) {
  google::bigtable::v2::MutateRowsRequest request;
  request.Swap(&r);
  if (request.entries_size() == 0) {
    return;
  }

  auto channel = grpc::CreateChannel("bigtable.googleapis.com", credentials_);
  auto bt_stub = google::bigtable::v2::Bigtable::NewStub(channel);

  auto backoff = boc.initial_backoff();
  char const* retry_msg = "retrying .";
  for (int i = 0; i != boc.max_retries(); ++i) {
    // ... accumulate the entries for the next retry ...
    google::bigtable::v2::MutateRowsRequest tmp;
    // ... accumulate any permanent errors in this request ...
    std::ostringstream os;

    // ... MutateRows() is a streaming RPC call, make the call and read from
    // the stream ...
    grpc::ClientContext ctx;
    auto stream = bt_stub->MutateRows(&ctx, request);
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
    // ... check if there was an error in the connection ...
    auto status = stream->Finish();
    if (not status.ok()) {
      // ... if it cannot be retried, abort ...
      if (not should_retry(status.error_code())) {
        os << "permanent error in MutateRow() request: " << status.error_message()
           << " [" << status.error_code() << "] " << status.error_details();
        throw std::runtime_error(os.str());
      }
      // ... if the operation can be retried we need to prepare the
      // next request, but there are not going to be ny
      auto channel = grpc::CreateChannel("bigtable.googleapis.com", credentials_);
      bt_stub = google::bigtable::v2::Bigtable::NewStub(channel);
      // What if we only got a response about a few entries and then a
      // response that indicates the whole operation should be
      // retried?
      // TODO(coryan) - we need to deal with such partial failures.
    }
    if (tmp.entries_size() == 0) {
      // ... nothing to retry, just return ...
      if (i > 0) {
        std::cout << " done" << std::endl;
      }
      return;
    }
    // ... prepare the next request ...
    tmp.mutable_table_name()->swap(*request.mutable_table_name());
    tmp.Swap(&request);
    backoff = backoff * 2;
    if (backoff > boc.maximum_backoff()) {
      backoff = boc.maximum_backoff();
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

} // namespace bigtable_api_samples
