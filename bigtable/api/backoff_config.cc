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

#include "backoff_config.h"

namespace bigtable_api_samples {

using namespace std::chrono_literals;

#ifndef BIGTABLE_API_SAMPLES_DEFAULT_MAX_RETRIES
#define BIGTABLE_API_SAMPLES_DEFAULT_MAX_RETRIES 100
#endif // BIGTABLE_API_SAMPLES_DEFAULT_MAX_RETRIES

#ifndef BIGTABLE_API_SAMPLES_DEFAULT_INITIAL_BACKOFF
#define BIGTABLE_API_SAMPLES_DEFAULT_INITIAL_BACKOFF 10ms
#endif // BIGTABLE_API_SAMPLES_DEFAULT_INITIAL_BACKOFF

#ifndef BIGTABLE_API_SAMPLES_DEFAULT_MAXIMUM_BACKOFF
#define BIGTABLE_API_SAMPLES_DEFAULT_MAXIMUM_BACKOFF 5min
#endif // BIGTABLE_API_SAMPLES_DEFAULT_MAXIMUM_BACKOFF

#ifndef BIGTABLE_API_SAMPLES_DEFAULT_JITTER_PERCENT
#define BIGTABLE_API_SAMPLES_DEFAULT_JITTER_PERCENT 25
#endif // BIGTABLE_API_SAMPLES_DEFAULT_JITTER_PERCENT

backoff_config::backoff_config()
    : max_retries_(BIGTABLE_API_SAMPLES_DEFAULT_MAX_RETRIES),
      initial_backoff_(std::chrono::duration_cast<std::chrono::microseconds>(
          BIGTABLE_API_SAMPLES_DEFAULT_INITIAL_BACKOFF)),
      maximum_backoff_(std::chrono::duration_cast<std::chrono::microseconds>(
          BIGTABLE_API_SAMPLES_DEFAULT_MAXIMUM_BACKOFF)),
      jitter_percent_(BIGTABLE_API_SAMPLES_DEFAULT_JITTER_PERCENT) {}

} // namespace bigtable_api_samples
