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

#include <iostream>

#include "data.h"

namespace btproto = ::google::bigtable::v2;

namespace bigtable {

std::unique_ptr<Table> Client::Open(const std::string& table_id) {
  std::string table_name = std::string("projects/") + project_ +
                           "/instances/" + instance_ + "/tables/" + table_id;
  std::unique_ptr<Table> table(new Table(this, table_name));
  return table;
}

grpc::Status Table::Apply(const std::string& row, Mutation& mutation) {
  btproto::MutateRowRequest request;
  request.set_table_name(table_name_);
  request.set_row_key(row);
  request.mutable_mutations()->Swap(&mutation.ops());

  btproto::MutateRowResponse response;
  grpc::ClientContext client_context;
  grpc::Status status = client_->Stub().MutateRow(
      &client_context, request, &response);
  return status;
}

grpc::Status Table::ReadRowsFromStream(
    grpc::ClientReaderInterface<google::bigtable::v2::ReadRowsResponse> *stream,
    std::function<bool(const RowPart &)> row_callback,
    std::function<void()> cancel_request) {
  btproto::ReadRowsResponse response;
  RowPart row;
  Cell cell;

  while (stream->Read(&response)) {
    for (auto& chunk : response.chunks()) {
      if (not chunk.row_key().empty()) {
        row.set_row(chunk.row_key());
      }
      if (chunk.has_family_name()) {
        cell.family = chunk.family_name().value();
      }
      if (chunk.has_qualifier()) {
        cell.column = chunk.qualifier().value();
      }
      cell.timestamp = chunk.timestamp_micros();
      if (chunk.value_size() > 0) {
        cell.value.reserve(chunk.value_size());
      }
      cell.value.append(chunk.value());
      if (chunk.value_size() == 0) {
        row.AddCell(cell);
        cell.value = "";
      }
      if (chunk.reset_row()) {
        row.Reset();
        cell = {};
      } else if (chunk.commit_row()) {
        // pass the row to the callback
        if (not row_callback(row)) {
          // stop request: cancel and drain the stream
          cancel_request();
          while (stream->Read(&response)) {}
          break;
        }
        row.Reset();
      }
    }
  }
  return stream->Finish();
}

grpc::Status Table::ReadRows(
    const RowSet& rows,
    std::function<bool(const RowPart &)> row_callback) {

  btproto::ReadRowsRequest request;
  request.set_table_name(table_name_);

  grpc::ClientContext context;
  return ReadRowsFromStream(client_->Stub().ReadRows(&context, request).get(),
                            row_callback,
                            [&context](){ context.TryCancel(); });
}

void Mutation::Set(const std::string& family,
                   const std::string& column,
                   int64_t timestamp,
                   const std::string& value) {
  auto* set_cell = ops_.Add()->mutable_set_cell();
  set_cell->set_family_name(family);
  set_cell->set_column_qualifier(column);
  set_cell->set_value(value);
  set_cell->set_timestamp_micros(timestamp);
}

void Mutation::DeleteCellsInColumn(const std::string& family,
                                   const std::string& column) {
  auto* set_cell = ops_.Add()->mutable_delete_from_column();
  set_cell->set_family_name(family);
  set_cell->set_column_qualifier(column);
}

}  // namespace bigtable
