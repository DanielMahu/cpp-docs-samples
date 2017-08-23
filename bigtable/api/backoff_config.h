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

#ifndef backoff_config_h
#define backoff_config_h

#include <chrono>
#include <stdexcept>

namespace bigtable_api_samples {
class backoff_config {
 public:
  backoff_config();

  int max_retries() const {
    return max_retries_;
  }
  backoff_config& set_max_retries(int v) {
    if (v < 0) {
      throw std::invalid_argument("max_retries should be >= 0");
    }
    max_retries_ = v;
    return *this;
  }

  std::chrono::microseconds initial_backoff() const {
    return initial_backoff_;
  }
  template<typename duration_type>
  backoff_config& set_initial_backoff(duration_type d) {
    using namespace std::chrono;
    initial_backoff_ = duration_cast<microseconds>(d);
    return *this;
  }

  std::chrono::microseconds maximum_backoff() const {
    return maximum_backoff_;
  }
  template<typename duration_type>
  backoff_config& set_maximum_backoff(duration_type d) {
    using namespace std::chrono;
    maximum_backoff_ = duration_cast<microseconds>(d);
    return *this;
  }
  
  int jitter_percent() const {
    return jitter_percent_;
  }
  backoff_config& set_jitter_percent(int v) {
    if (v < 0 or v > 100) {
      throw std::invalid_argument("jitter_percent should be in [0,100] range");
    }
    jitter_percent_ = v;
    return *this;
  }

private:
  int max_retries_;
  std::chrono::microseconds initial_backoff_;
  std::chrono::microseconds maximum_backoff_;
  int jitter_percent_;
};
} // namespace bigtable_api_samples

#endif // backoff_config_h
