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

#include "admin.h"

namespace bigtable {

void TableConfiguration::AddColumnFamily(const std::string& name,
                                         const GCRule& gc_rule) {
  names_.push_back(name);
  gc_rules_.push_back(gc_rule);
}

grpc::Status AdminClient::CreateTable(const std::string& table_name) {
  return CreateTable(table_name, {});
}

grpc::Status AdminClient::CreateTable(
    const std::string& table_name,
    const TableConfiguration& config) {
  btaproto::CreateTableRequest request;
  request.set_parent(std::string("projects/") + project_ +
      "/instances/" + instance_);
  request.set_table_id(table_name);

  auto gc = config.gc_rules().begin();
  for (const auto& family : config.names()) {
    auto& fam = (*request.mutable_table()->mutable_column_families())[family];
    gc->UpdateProto(fam.mutable_gc_rule());
    gc++;
  }

  btaproto::Table response;
  grpc::ClientContext client_context;
  grpc::Status status = bt_stub_->CreateTable(
      &client_context, request, &response);

  return status;
}

}  // namespace bigtable
