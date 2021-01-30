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

#include <lz4.h>
#include <zstd.h>

#include "common/Hash.h"
#include "common/Memory.h"

// Count-Min is an algorithm to accumulate frequency
// and expose API to fetch frequency at low bound values
// The algo is described well in
// https://www.slideshare.net/gakhov/probabilistic-data-structures-part-3-frequency

namespace nebula {
namespace common {

// H - number of hash function pass
// M - number of slots to hold data
// CT - count type, many cases require 4 bytes or even 2 bytes
template <size_t H = 5, size_t M = 1023, typename CT = size_t>
class CountMin {
  // SIZE in bytes = CELLS (H * M) * TYPE_SIZE
  static constexpr auto SIZE = H * M * sizeof(CT);

public:
  // initialize count min sketch with map size (m) and hash size (h)
  CountMin() : total_{ 0 } {
    // initialize table with 0
    for (size_t r = 0; r < H; ++r)
      for (size_t c = 0; c < M; ++c) {
        table_[r][c] = 0;
      }
  }
  ~CountMin() = default;

public:
  template <typename T = size_t>
  void add(T v, CT count = 1) {
    // increment total counter
    total_ += count;

    // get N hashes for given value
    auto hashes = nebula::common::Hasher::hash64<H>(&v, sizeof(T));

    // distribute each hash to the table
    for (size_t i = 0; i < H; ++i) {
      table_[i][hashes.at(i) % M] += count;
    }
  }

  // query given value
  template <typename T = size_t>
  CT query(T v) const {
    auto hashes = nebula::common::Hasher::hash64<H>(&v, sizeof(T));
    // distribute each hash to the table
    CT min = std::numeric_limits<CT>::max();
    for (size_t i = 0; i < H; ++i) {
      auto x = table_[i][hashes.at(i) % M];
      if (x < min) {
        min = x;
      }
    }

    return min;
  }

  // merge another sketch sharing the same spec HxM
  void merge(const CountMin<H, M, CT>& other) {
    for (size_t r = 0; r < H; ++r)
      for (size_t c = 0; c < M; ++c) {
        auto cell = other.table_[r][c];
        table_[r][c] += cell;
        total_ += cell;
      }
  }

  static inline std::string name() {
    return fmt::format("<{0}, {1}, {2}>", H, M, typeid(CT).name());
  }

  inline size_t count() const {
    return total_;
  }

  inline size_t size() const {
    return SIZE;
  }

  inline std::unique_ptr<nebula::common::OneSlice> compress() const {
    // TODO(cao) - this is true AFAIK that 2D array memory layout is continuous.
    // so that we can treat it as a single memory chunk.
    // ref: http://www.cplusplus.com/doc/tutorial/arrays/
    const auto address = (char*)&table_[0][0];
    auto slice = std::make_unique<nebula::common::OneSlice>(SIZE);

    // LZ4 vs ZSTD
    auto size = ZSTD_compress((char*)slice->ptr(), SIZE, address, SIZE, 9);
    // size_t size = LZ4_compress_default(address, (char*)slice->ptr(), SIZE, SIZE);

    // not good to compress, keep it as raw
    if (ZSTD_isError(size)) {
      // if (size == 0) {
      std::memcpy(slice->ptr(), address, SIZE);
      return slice;
    }

    // copy into a smaller buffer
    auto fit = std::make_unique<nebula::common::OneSlice>(size);
    std::memcpy(fit->ptr(), slice->ptr(), size);
    return fit;
  }

private:
  CT table_[H][M];
  size_t total_;
};
} // namespace common
} // namespace nebula