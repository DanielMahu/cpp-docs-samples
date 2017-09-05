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

#ifndef BIGTABLE_CLIENT_DATA_H_
#define BIGTABLE_CLIENT_DATA_H_

#include <string>

#include "backoff_config.h"

#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <grpc++/grpc++.h>

namespace bigtable {

// ... save ourselves some typing ...
namespace btproto = ::google::bigtable::v2;

class Table;

class Client {
 public:
  Client(std::shared_ptr<grpc::ChannelCredentials> credentials)
      : credentials_(credentials),
        channel_(grpc::CreateChannel("bigtable.googleapis.com", credentials)),
        bt_stub_(btproto::Bigtable::NewStub(channel_)) {}

  // Default constructor uses the default credentials
  Client() : Client(grpc::GoogleDefaultCredentials()) {}

  // Create a Table object for use with the Data API. Never fails, all
  // error checking happens during operations.
  std::unique_ptr<Table> Open(const std::string& table_name);

 private:
  std::shared_ptr<grpc::ChannelCredentials> credentials_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<btproto::Bigtable::Stub> bt_stub_;

  friend class Table;
};

class Mutation {
 public:
  void Set(const std::string& family,
           const std::string& column,
           int timestamp,
           const std::string& value);

  void DeleteCellsInColumn(const std::string& family,
                           const std::string& column);

  google::protobuf::RepeatedPtrField<btproto::Mutation>& GetOps() {
    return ops_;
  }

 private:
  google::protobuf::RepeatedPtrField<btproto::Mutation> ops_;
};

class Table {
 public:
  Table(const Client *client, const std::string& table_name)
    : client_(client),
      table_name_(table_name) {}

  const std::string& GetTableName() const {
    return table_name_;
  }

  grpc::Status Apply(const std::string& row, Mutation& mutation);

 private:
  const Client *client_;
  std::string table_name_;
};

}  // namespace bigtable

#endif  // BIGTABLE_CLIENT_DATA_H_
