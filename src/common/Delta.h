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

#include <cmath>
#include <condition_variable>

#include "Evidence.h"
#include "Memory.h"

/**
 * Delta encoding takes input of an array of data with sortable type.
 * It scans the whole array sequentially as processing. 
 * Whenever it finds a chunkable range of values, it will save the first one as base, the rest as delta.
 * Because deltas are usually small, they can be represented by less bytes using variant encoding.
 * To increase variant encoding speed, group variant encoding could be leveraged (2x faster).
 * 
 */

namespace nebula {
namespace common {

// max delta we can accept values to be clustered
constexpr auto MAX_DELTA = std::numeric_limits<uint16_t>::max();
constexpr auto EST_RATIO = 0.4;

// memory buffer used for bytes stream
struct Buffer {
  explicit Buffer(size_t size)
    : slice{ std::make_unique<ExtendableSlice>(size) },
      written{ 0 } {}

  std::unique_ptr<ExtendableSlice> slice;
  // number of bytes written in slice
  size_t written;
};

/**
   * Reads a varint from the given InputStream and returns the decoded value
   * as an int.
   *
   * @param data the bytes stream to decode next int
   */
static inline int32_t getVarInt(int8_t* data, size_t& i) {
  int32_t result = 0;
  int32_t shift = 0;
  int32_t b;
  do {
    // Out of range
    N_ENSURE_LT(shift, 32, "corrupted data");

    // Get 7 bits from next byte
    b = *(data + i++);
    result |= (b & 0x7F) << shift;
    shift += 7;
  } while ((b & 0x80) != 0);

  return result;
}

/**
   * Encodes an integer in a variable-length encoding, 7 bits per byte, into a
   * destination byte[], following the protocol buffer convention.
   *
   * @param v the int value to write to sink
   * @param buf the sink buffer (auto growable) to write to
   */
static inline void putVarInt(int32_t sv, Buffer& buf) {
  // we need unsigned right shift
  uint32_t v = (uint32_t)sv;
  do {
    // Encode next 7 bits + terminator bit
    int32_t bits = v & 0x7F;
    v >>= 7;
    int8_t b = (bits + ((v != 0) ? 0x80 : 0));
    buf.written += buf.slice->write<int8_t>(buf.written, b);
  } while (v != 0);
}

/*
 * Input data is either sorted or unsorted, this method will sort it anyways.
 * Size is the count of T values, not byte length.
 * Support 4bytes or 8 bytes integer only
 */
template <typename T>
auto delta_encode(T* data, size_t size) -> std::enable_if_t<
  std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>,
  Buffer> {
  constexpr auto WIDTH = sizeof(T);

  // sort the input array.
  std::sort(data, data + size);

  // allocate the output slice assuming 60% reduction
  Buffer buf{ (size_t)(size * WIDTH * EST_RATIO) };

  // loop values and figure out encoding window and encode them
  size_t i = 0;
  while (i < size) {
    // first value is the base
    T base = *(data + i);
    auto run = i + 1;

    // TODO(cao): optimization=we probably can merge below two whiles in one pass
    while (run < size && (*(data + run) - base) < MAX_DELTA) {
      run++;
    }

    // flag indicating if it has runs
    const auto runs = run - i - 1;

    // no runs, then this is a single value
    // 8 bytes,we write 2 x 4 bytes
    if constexpr (WIDTH == 8) {
      auto high = (int32_t)(base >> 32);
      auto low = (int32_t)base;

      // write base
      putVarInt(high, buf);
      putVarInt(low, buf);
    }

    if constexpr (WIDTH == 4) {
      putVarInt(base, buf);
    }

    // write 4 bytes run length
    putVarInt((int32_t)runs, buf);

    // move to next value
    i++;

    // write runs
    if (runs > 0) {
      // write all deltas in variant
      while (i < run) {
        putVarInt(*(data + i++) - base, buf);
      }
    }
  }

  // return this slice.
  return buf;
}

/*
 * Input data is either sorted or unsorted, this method will sort it anyways.
 * Size is the count of T values, not byte length.
 * Support 4bytes or 8 bytes integer only
 */
template <typename T>
auto delta_decode(int8_t* data, size_t size) -> std::enable_if_t<
  std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>,
  Buffer> {
  if (size == 0) {
    return Buffer{ 0 };
  }

  constexpr auto WIDTH = sizeof(T);
  // read all the bytes and decode them into T values saved in buffer
  Buffer buf{ (size_t)(size / EST_RATIO) };
  size_t i = 0;
  while (i < size) {
    // read base
    int64_t base;
    if constexpr (WIDTH == 8) {
      int64_t high = getVarInt(data, i);
      int32_t low = getVarInt(data, i);
      base = (high << 32) | low;
    }

    if constexpr (WIDTH == 4) {
      base = getVarInt(data, i);
    }

    // write base value
    buf.written += buf.slice->write<T>(buf.written, (T)base);

    // read runs
    auto runs = getVarInt(data, i);
    for (auto k = 0; k < runs; ++k) {
      auto v = base + getVarInt(data, i);
      buf.written += buf.slice->write<T>(buf.written, (T)v);
    }
  }

  N_ENSURE(i == size, "corrupted data.");
  return buf;
}

} // namespace common
} // namespace nebula