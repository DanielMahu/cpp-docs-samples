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

#ifndef client_h
#define client_h

#include "backoff_config.h"

#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <grpc++/grpc++.h>

namespace bigtable_api_samples {

class client {
public:
  client(std::shared_ptr<grpc::ChannelCredentials> credentials)
    : credentials_(credentials) {
  }

  void mutate_rows(google::bigtable::v2::MutateRowsRequest&& req,
                   backoff_config const& backoff);
private:

private:
  std::shared_ptr<grpc::ChannelCredentials> credentials_;
};

} // namespace bigtable_api_samples

#endif // client_h
