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

class Table;

class Client {
 public:
  Client(const std::string& project,
         const std::string& instance,
         std::shared_ptr<grpc::ChannelCredentials> credentials)
      : project_(project),
        instance_(instance),
        credentials_(credentials),
        channel_(grpc::CreateChannel("bigtable.googleapis.com", credentials)),
        bt_stub_(google::bigtable::v2::Bigtable::NewStub(channel_)) {}

  Client(const std::string& project,
         const std::string& instance)
      : Client(project, instance, grpc::GoogleDefaultCredentials()) {}

  // Create a Table object for use with the Data API. Never fails, all
  // error checking happens during operations.
  std::unique_ptr<Table> Open(const std::string& table_id);

  google::bigtable::v2::Bigtable::StubInterface& Stub() const {
    return *bt_stub_;
  }

 private:
  std::string project_;
  std::string instance_;
  std::shared_ptr<grpc::ChannelCredentials> credentials_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<google::bigtable::v2::Bigtable::StubInterface> bt_stub_;

  friend class Table;
};

// A mutation is made up from several operations. Member functions in
// this class each add a mutation to the list of operations to
// execute. The list is applied atomically for a row and in order.
class Mutation {
 public:
  // Set a single cell at a timestamp to the value given.
  void Set(const std::string& family,
           const std::string& column,
           int64_t timestamp,
           const std::string& value);

  // Delete all values in family:column.
  void DeleteCellsInColumn(const std::string& family,
                           const std::string& column);

  google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation>& ops() {
    return ops_;
  }

 private:
  google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> ops_;
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

  // Internal functions; clients should not call these, which is
  // promoted by always returning const values
  void AddCell(const Cell& cell) { cells_.push_back(cell); }
  void Reset() { cells_.clear(); }

 private:
  std::vector<Cell> cells_;
  std::string row_;
};

class Table {
 public:
  class ReadStream {
   public:
    class iterator : public std::iterator<std::input_iterator_tag, RowPart> {
     public:
      iterator(ReadStream* owner, bool is_end)
        : owner_(owner), is_end_(is_end) { }

      iterator& operator++();
      const RowPart& operator*() { return owner_->row_; }
      const RowPart* operator->() { return &owner_->row_; }
      bool operator==(const iterator& that) const;
      bool operator!=(const iterator& that) const { return !(*this == that); }
     private:
      ReadStream* const owner_;
      bool is_end_;
    };

    typedef iterator iterator_type;
    typedef const RowPart value_type;

    // When constructing, a full row is read immediately. This is one
    // way to make the iterator returned by begin() valid.
    ReadStream(std::unique_ptr<grpc::ClientContext> context,
               std::unique_ptr<grpc::ClientReaderInterface<
                    google::bigtable::v2::ReadRowsResponse>> stream)
        : context_(std::move(context)),
          stream_(std::move(stream)),
          response_is_valid_(false),
          is_at_end_(false),
          begin_iter_(this, false),
          end_iter_(this, true) {
      Advance();
    }

    iterator& begin() { return begin_iter_; }
    iterator& end() { return end_iter_; }

    void Cancel();
    grpc::Status FinalStatus();

   private:
    void Advance();
    void AdvanceChunk();
    bool AtEnd() { return is_at_end_; }

    std::unique_ptr<grpc::ClientContext> context_;
    std::unique_ptr<grpc::ClientReaderInterface<
                    google::bigtable::v2::ReadRowsResponse>> stream_;

    // last read message from the stream
    google::bigtable::v2::ReadRowsResponse response_;
    // parser state
    bool response_is_valid_;
    int chunk_;
    Cell cell_;

    // a complete row, but invalid if is_at_end_
    RowPart row_;
    // end of stream was reached
    bool is_at_end_;

    iterator begin_iter_;
    iterator end_iter_;
  };

  Table(const Client *client, const std::string& table_name)
    : client_(client),
      table_name_(table_name) {}

  const std::string& table_name() const { return table_name_; }

  // Attempts to apply the mutation to a row and returns the status
  // from the call. The mutation argument may be cleared when this
  // call returns.
  grpc::Status Apply(const std::string& row, Mutation& mutation);

  // Returns a stream wrapper that can be used to iterate over the
  // returned rows. Tries to parse a first row before returning.
  std::unique_ptr<ReadStream> ReadRows(const RowSet& row_filter);

 protected:
  // Protected for testability
  grpc::Status ReadRowsFromStream(
    grpc::ClientReaderInterface<google::bigtable::v2::ReadRowsResponse> *stream,
    std::function<bool(const RowPart &)> row_callback,
    std::function<void()> cancel_request);

 private:
  const Client *client_;
  std::string table_name_;
};

}  // namespace bigtable

#endif  // BIGTABLE_CLIENT_DATA_H_
