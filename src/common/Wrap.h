/*
 * Copyright 2017-present varchar.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <glog/logging.h>
#include <vector>

#include "Likely.h"

/**
 * Defines some wrappers for common functions for guarding or instrumentation purpose.
 */
namespace nebula {
namespace common {

template <typename T, typename S>
inline bool vector_reserve(std::vector<T>& vec, const S size, const std::string& site) noexcept {
  // it's impossible for a vector to hold 1M items or maybe it's possible but not practical.
  // so let's guard it here for Nebula - could be revise with reasons.
  if (N_UNLIKELY(size <= 0 || size >= 1000000)) {
    if (size != 0) {
      LOG(WARNING) << "Invalid vector size " << size << " at " << site;
    }
    return false;
  }

  vec.reserve(size);
  return true;
}

} // namespace common
} // namespace nebula