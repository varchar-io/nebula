/*
 * Copyright 2017-present Shawn Cao
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

#include <string>
#include <xxh3.h>

/**
 * A wrapper to decide which hash functions to be used in nebula.
 * It may have differnet variants to apply for different scenarios.
 * So expect more than one API.
 */

namespace nebula {
namespace common {

class Hasher {
public:
  XXH_FORCE_INLINE size_t hashString(const std::string_view& sv) {
    return hash64(sv.data(), sv.size());
  }

  XXH_FORCE_INLINE size_t hashString(const std::string& str) {
    return hash64(str.data(), str.size());
  }

  XXH_FORCE_INLINE size_t hash64(const void* p, const size_t len) {
    return XXH3_64bits(p, len);
  }
};

} // namespace common
} // namespace nebula