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
#include <functional>
#include <vector>

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

  google::protobuf::RepeatedPtrField<btproto::Mutation>& ops() {
    return ops_;
  }

 private:
  google::protobuf::RepeatedPtrField<btproto::Mutation> ops_;
};

// TODO(dmahu): this is a stub
class RowSet {
};

struct Cell {
  std::string row;
  std::string family;
  std::string column;
  int64_t timestamp;

  std::string value;
};

// Row returned by a read call, might not contain all contents
// of the row -- depending on the filter applied
class RowPart {
 public:
  using const_iterator = std::vector<Cell>::const_iterator;

  const std::string& row() const { return row_; }

  // Allow direct iteration over cells.
  const_iterator begin() const { return cells_.cbegin(); }
  const_iterator end() const { return cells_.cend(); }

  void set_row(const std::string& row) { row_ = row; }

  // Internal functions
  void AddCell(const Cell& cell) { cells_.push_back(cell); }
  void Reset() { cells_.clear(); }

 private:
  std::vector<Cell> cells_;
  std::string row_;
};

class Table {
 public:
  Table(const Client *client, const std::string& table_name)
    : client_(client),
      table_name_(table_name) {}

  const std::string& table_name() const {
    return table_name_;
  }

  grpc::Status Apply(const std::string& row, Mutation& mutation);

  grpc::Status ReadRows(const RowSet& rows,
                        std::function<bool(const RowPart &)> f);

 private:
  const Client *client_;
  std::string table_name_;
};

}  // namespace bigtable

#endif  // BIGTABLE_CLIENT_DATA_H_
