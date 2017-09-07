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

#ifndef BIGTABLE_CLIENT_ADMIN_H_
#define BIGTABLE_CLIENT_ADMIN_H_

#include <string>
#include <functional>
#include <vector>

#include <google/bigtable/admin/v2/bigtable_table_admin.grpc.pb.h>
#include <grpc++/grpc++.h>

namespace bigtable {

// ... save ourselves some typing ...
namespace btaproto = ::google::bigtable::admin::v2;

// This is a garbage collection rule for a column family.
class GCRule {
 public:
  // TODO(dmahu): GCRule is a stub

  // Internally used
  void UpdateProto(btaproto::GcRule* proto_rule) const {}
};

// This class is used to pass configuration to the CreateTable() call.
class TableConfiguration {
 public:
  TableConfiguration() {}

  // Add a column family with specific garbage collection rules.
  void AddColumnFamily(const std::string& name,
                       const GCRule& gc_rule);

  // Add a column family with default garbage collection.
  void AddColumnFamily(const std::string& name) {
    AddColumnFamily(name, {});
  }

  const std::vector<const std::string> names() const { return names_; }
  const std::vector<GCRule> gc_rules() const { return gc_rules_; }

 private:
  std::vector<const std::string> names_;
  std::vector<GCRule> gc_rules_;
};

class AdminClient {
 public:
  AdminClient(const std::string& project,
              const std::string& instance,
              std::shared_ptr<grpc::ChannelCredentials> credentials)
      : project_(project),
        instance_(instance),
        credentials_(credentials),
        channel_(grpc::CreateChannel("bigtableadmin.googleapis.com", credentials)),
        bt_stub_(btaproto::BigtableTableAdmin::NewStub(channel_)) {}

  // Default constructor uses the default credentials
  AdminClient(const std::string& project,
              const std::string& instance)
      : AdminClient(project, instance, grpc::GoogleDefaultCredentials()) {}

  // Creates an empty table.
  grpc::Status CreateTable(const std::string& table_name);

  // Creates a table from a TableConfiguration.
  grpc::Status CreateTable(const std::string& table_name,
                           const TableConfiguration& config);

 private:
  std::string project_;
  std::string instance_;
  std::shared_ptr<grpc::ChannelCredentials> credentials_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<btaproto::BigtableTableAdmin::Stub> bt_stub_;
};

}  // namespace bigtable

#endif  // BIGTABLE_CLIENT_ADMIN_H_
