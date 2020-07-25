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

#include <array>
#include <string>
#include <xxh3.h>
#include "robin_hood.h"

/**
 * A wrapper to decide which hash functions to be used in nebula.
 * It may have differnet variants to apply for different scenarios.
 * So expect more than one API.
 */
namespace nebula {
namespace common {

class Hasher {
public:
  XXH_FORCE_INLINE size_t hashString(const std::string_view& sv) noexcept {
    return hash64(sv.data(), sv.size());
  }

  XXH_FORCE_INLINE size_t hashString(const std::string& str) noexcept {
    return hash64(str.data(), str.size());
  }

  XXH_FORCE_INLINE size_t hash64(const void* p, const size_t len) noexcept {
    // In TestHash/TestHashFunc, looks like robin_hood::hash_bytes is 2x faster
    // Use it for now, and I haven't get into the detaills of the diff yet.
    return robin_hood::hash_bytes(p, len);
  }

  template <size_t N>
  XXH_FORCE_INLINE std::array<size_t, N> hash64(const void* p, size_t len) noexcept {
    XXH3_state_t state;
    if (XXH3_64bits_reset(&state) == XXH_ERROR) {
      return {};
    }

    std::array<size_t, N> hashes;

    for (size_t i = 0; i < N; ++i) {
      XXH3_64bits_update(&state, p, len);
      hashes[i] = (XXH3_64bits_digest(&state));
    }

    return hashes;
  }
};

// wrapper for unordered_map and unordered_set
// based on performance testing, see test/TestHash.cpp
// we change choice here as they all have consistent interfaces.
// we ignored other defaulted template parameters -
// we may add them back for default tuning
template <typename Key,
          typename T,
          typename Hash = robin_hood::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
using unordered_map = robin_hood::unordered_map<Key, T, Hash, KeyEqual>;

template <typename Key,
          typename Hash = robin_hood::hash<Key>,
          typename KeyEqual = std::equal_to<Key>,
          size_t MaxLoadFactor100 = 80>
using unordered_set = robin_hood::unordered_set<Key, Hash, KeyEqual, MaxLoadFactor100>;

} // namespace common
} // namespace nebula