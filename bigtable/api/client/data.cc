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

namespace bigtable {

std::unique_ptr<Table> Client::Open(const std::string& table_name) {
  std::unique_ptr<Table> table(new Table(this, table_name));
  return table;
}

grpc::Status Table::Apply(const std::string& row, Mutation& mutation) {
  btproto::MutateRowRequest request;
  request.set_table_name(table_name_);
  request.set_row_key(row);
  request.mutable_mutations()->Swap(&mutation.GetOps());

  btproto::MutateRowResponse response;
  grpc::ClientContext client_context;
  grpc::Status status = client_->bt_stub_->MutateRow(
      &client_context, request, &response);
  return status;
}

void Mutation::Set(const std::string& family,
                   const std::string& column,
                   int timestamp,
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
