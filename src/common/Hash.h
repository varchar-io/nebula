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

class Murmur3 {
  static constexpr auto HLL_HASH_SEED = 313;

public:
  static inline uint32_t getblock(const uint32_t* p, int i) {
    const auto x = p[i];
    /* NO-OP for little-endian platforms */
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) \
  && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return x;

// if __BYTE_ORDER__ is not predefined (like FreeBSD), use arch
#elif defined(__i386) || defined(__x86_64) \
  || defined(__alpha) || defined(__vax)

    return x;

// use __builtin_bswap32 if available
#elif (defined(__GNUC__) || defined(__clang__)) \
  && defined(__has_builtin) && __has_builtin(__builtin_bswap32)
    return __builtin_bswap32(x);
#else

    // last resort (big-endian w/o __builtin_bswap)
    return ((((x)&0xFF) << 24)
            | (((x) >> 24) & 0xFF)
            | (((x)&0x0000FF00) << 8)
            | (((x)&0x00FF0000) >> 8));

#endif
  }

  static inline uint32_t rotl32(uint32_t x, uint8_t r) {
    return (x << r) | (x >> (32 - r));
  }

  static inline uint32_t fmix32(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
  }

  static uint32_t hash(const char* data, size_t size, uint32_t seed = HLL_HASH_SEED) {
    const auto nblocks = size / 4;
    constexpr uint32_t c1 = 0xcc9e2d51;
    constexpr uint32_t c2 = 0x1b873593;
    constexpr uint32_t c3 = 0xe6546b64;

    uint32_t h1 = seed;
    //----------
    // body
    const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);

    for (int i = -nblocks; i; i++) {
      uint32_t k1 = getblock(blocks, i);

      k1 *= c1;
      k1 = rotl32(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = rotl32(h1, 13);
      h1 = h1 * 5 + c3;
    }

    //----------
    // tail
    {
      const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);

      uint32_t k1 = 0;

      switch (size & 3) {
      case 3: k1 ^= tail[2] << 16;
      case 2: k1 ^= tail[1] << 8;
      case 1:
        k1 ^= tail[0];
        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;
        h1 ^= k1;
      };
    }

    //----------
    // finalization
    h1 ^= size;
    return fmix32(h1);
  }
};

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