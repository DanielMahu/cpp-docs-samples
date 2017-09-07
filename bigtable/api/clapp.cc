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

#include <grpc++/grpc++.h>
#include <iostream>

// TODO(dmahu): wanted to have <bigtable/client/___.h> here
#include <client/data.h>
#include <client/admin.h>


std::string error_details(const grpc::Status& status) {
  return status.error_message() + " [" +
      std::to_string(status.error_code()) + "] " +
      status.error_details();
}


#define RET_IF_FAIL(call) {                                            \
  grpc::Status status = (call);                                        \
  if (not status.ok()) {                                               \
    std::cerr << "Error in "#call": " << error_details(status) << "\n";\
    return 1;                                                          \
  } }


int main(int argc, char* argv[]) {
  if (argc != 6) {
    std::cerr
        << "Usage: mutate <project_id> <instance_id> <table> <row> <action>"
        << std::endl;
    return 1;
  }
  char const* project_id = argv[1];
  char const* instance_id = argv[2];
  char const* table_id = argv[3];
  char const* row = argv[4];
  char const* action = argv[5];

  char const* family = "cf1";
  char const* column = "example";

  // Part 1: create a table

  bigtable::AdminClient admin(project_id, instance_id);  // default credentials
  bigtable::TableConfiguration config;

  // We add the column family at table creation. If the table already
  // exists, but does not have a column family with this name, the
  // mutation calls below will fail.
  config.AddColumnFamily(family);

  // Table creation should be a rare event; we are ignoring errors if
  // the table already exists.
  {
    grpc::Status status = admin.CreateTable(table_id, config);
    if (not status.ok() and
        (status.error_code() != grpc::ALREADY_EXISTS)) {
      std::cerr << "Error in CreateTable: " << error_details(status) << "\n";
      return 1;
    }
  }

  // Part 2: set or delete, then read
  bool should_set = !strncmp(action, "set", strlen("set"));
  bool should_delete = !strncmp(action, "delete", strlen("delete"));

  bigtable::Client client;  // with no arguments, uses default credentials

  std::string table_name = std::string("projects/") + project_id +
                           "/instances/" + instance_id + "/tables/" + table_id;

  std::unique_ptr<bigtable::Table> table = client.Open(table_name);

  bigtable::Mutation mutation;

  if (should_set) {
    mutation.Set(family, column, 42000, "a test value");
    std::cerr << "set row " << row << "\n";
  }
  if (should_delete) {
    mutation.DeleteCellsInColumn(family, column);
    std::cerr << "delete row " << row << "\n";
  }

  RET_IF_FAIL(table->Apply(row, mutation));

  bigtable::RowSet rs;
  RET_IF_FAIL(table->ReadRows(rs, [](const bigtable::RowPart &rp){
      std::cout << "row " << rp.row() << "\n";
      for (const auto& cell : rp) {
        std::cout << "  " << cell.family << ":"
                  << cell.column << "@"
                  << cell.timestamp << " = "
                  << cell.value << "\n";
      }
      return true;
    }));

  return 0;
}
