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

grpc::Status Table::ReadStream::FinalStatus() {
  return stream_->Finish();
}

void Table::ReadStream::Cancel() {
  context_->TryCancel();
  // Also drain any data left unread
  btproto::ReadRowsResponse response;
  while (stream_->Read(&response)) {}
  // Reached the end
  is_at_end_ = true;
}

// Moves the chunk index forward, reading another response if needed
void Table::ReadStream::AdvanceChunk() {
  if (!response_is_valid_ ||
      (response_is_valid_ && chunk_ == response_.chunks_size() - 1)) {
    response_is_valid_ = stream_->Read(&response_);
    chunk_ = 0;
  } else {
    chunk_++;
  }
}

// Moves forward one row at a time, setting is_at_end_ when the stream ends
void Table::ReadStream::Advance() {

  row_ = {};
  cell_ = {};

  while (1) {

    AdvanceChunk();
    if (!response_is_valid_) {
      is_at_end_ = true;
      return;
    }

    // TODO(dmahu): validate the reply; at the moment it is used blindly

    auto& chunk = response_.chunks(chunk_);

    if (not chunk.row_key().empty()) {
      row_.set_row(chunk.row_key());
    }
    if (chunk.has_family_name()) {
      cell_.family = chunk.family_name().value();
    }
    if (chunk.has_qualifier()) {
      cell_.column = chunk.qualifier().value();
    }
    cell_.timestamp = chunk.timestamp_micros();
    if (chunk.value_size() > 0) {
      cell_.value.reserve(chunk.value_size());
    }
    cell_.value.append(chunk.value());
    if (chunk.value_size() == 0) {
      row_.AddCell(cell_);
      cell_.value = "";
    }
    if (chunk.reset_row()) {
      row_ = {};
      cell_ = {};
    } else if (chunk.commit_row()) {
      return;
    }

  }
}

Table::ReadStream::iterator& Table::ReadStream::iterator::operator++() {
  owner_->Advance();
  return *this;
}

bool Table::ReadStream::iterator::operator==(
    const Table::ReadStream::iterator& other) const {
  if (owner_ != other.owner_)
    return false;
  // All iterators become equal at end of input
  if (owner_->AtEnd())
    return true;
  // otherwise we have just two classes
  return is_end_ == other.is_end_;
}

std::unique_ptr<Table::ReadStream> Table::ReadRows(
    const RowSet& rows) {
  btproto::ReadRowsRequest request;
  request.set_table_name(table_name_);
  auto context = std::make_unique<grpc::ClientContext>();
  return std::make_unique<Table::ReadStream>(
      std::move(context),
      client_->Stub().ReadRows(context.get(), request));
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
