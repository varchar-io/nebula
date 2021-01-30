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

#include <fmt/format.h>

namespace nebula {
namespace common {
// TODO(cao): this is not belonging to nebula.
// But for cross function, we're temporary capture how Spark shuffle hash function works
// This is a short-term workaround to have for bucketing data based on Spark repartition
class Spark {
  static constexpr auto C1 = 0xcc9e2d51;
  static constexpr auto C2 = 0x1b873593;
  static constexpr auto SEED = 42;

public:
  static inline int32_t hashInt(int32_t value, int32_t seed = SEED) {
    auto h = mixH(seed, mixK(value));
    return fmix(h, 4);
  }

  static inline int32_t hashLong(int64_t value, int32_t seed = SEED) {
    auto low = (int32_t)value;
    auto high = lrs<uint64_t>(value, 32);
    auto k = mixK(low);
    auto h = mixH(seed, k);
    k = mixK(high);
    h = mixH(h, k);
    return fmix(h, 8);
  }

  // use the method to find bucket for given value
  static inline uint32_t hashBucket(int64_t value, int32_t buckets) {
    auto b = hashLong(value) % buckets;
    if (b < 0) {
      return b + buckets;
    }

    return b;
  }

private:
  // logical right shift
  template <typename T = uint32_t>
  static inline int32_t lrs(T v, int32_t bits) {
    return (int32_t)(v >> bits);
  }

  static inline int32_t rotateLeft(uint32_t n, int32_t d) {
    // Integer.rotateLeft impl
    return (n << d) | lrs<uint32_t>(n, 32 - d);
  }

  static inline int32_t mixK(int32_t k) {
    k *= C1;
    k = rotateLeft(k, 15);
    k *= C2;
    return k;
  }

  static inline int32_t mixH(int32_t h, int32_t k) {
    h ^= k;
    h = rotateLeft(h, 13);
    h = h * 5 + 0xe6546b64;
    return h;
  }

  // Finalization mix - force all bits of a hash block to avalanche
  static int32_t fmix(int32_t h, int32_t length) {
    h ^= length;
    h ^= lrs<uint32_t>(h, 16);
    h *= 0x85ebca6b;
    h ^= lrs<uint32_t>(h, 13);
    h *= 0xc2b2ae35;
    h ^= lrs<uint32_t>(h, 16);
    return h;
  }
};
} // namespace common
} // namespace nebula