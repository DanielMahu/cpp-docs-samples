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

// TODO(dmahu): wanted to have <bigtable/client/data.h> here
#include <client/data.h>

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

  bool should_set = !strncmp(action, "set", strlen("set"));
  bool should_delete = !strncmp(action, "delete", strlen("delete"));

  bigtable::Client client;  // with no arguments, uses default credentials

  std::string table_name = std::string("projects/") + project_id +
                           "/instances/" + instance_id + "/tables/" + table_id;

  std::unique_ptr<bigtable::Table> table = client.Open(table_name);

  bigtable::Mutation mutation;

  if (should_set) {
    mutation.Set(family, column, 0, "a test value");
    std::cerr << "set row " << row << "\n";
  }
  if (should_delete) {
    mutation.DeleteCellsInColumn(family, column);
    std::cerr << "delete row " << row << "\n";
  }

  grpc::Status status = table->Apply(row, mutation);
  if (not status.ok()) {
    std::cerr << "Error in MutateRow() request: " << status.error_message()
                << " [" << status.error_code() << "] " << status.error_details()
                << std::endl;
    return 1;
  }

  return 0;
}
