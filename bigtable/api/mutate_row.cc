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

//#include <ciso646>
#include <iostream>

#include "data.h"

int main(int argc, char* argv[]) {
  if (argc != 5) {
    std::cerr
        << "Usage: mutate <project_id> <instance_id> <table> <row>"
        << std::endl;
    return 1;
  }
  char const* project_id = argv[1];
  char const* instance_id = argv[2];
  char const* table_id = argv[3];
  char const* row = argv[4];

  bigtable::Client client;  // with no arguments, uses default credentials

  std::string table_name = std::string("projects/") + project_id +
                           "/instances/" + instance_id + "/tables/" + table_id;

  bigtable::Mutation mutation;
  bigtable::Table table(&client, table_name);

  std::cerr << "should mutate row " << row << "\n";
  table.Apply(row, mutation);

  return 0;
}
