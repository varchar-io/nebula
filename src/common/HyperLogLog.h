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

#include <numeric>

#include "Hash.h"
#include "common/Errors.h"
#include "common/Memory.h"

namespace nebula {
namespace common {

/** 
 * Origin=https://github.com/hideo55/cpp-HyperLogLog/blob/master/include/hyperloglog.hpp
 * The file is modified to fit in Nebula project
 * 
 * @brief HyperLogLog cardinality estimator
 * @date Created 2013/3/20
 * @author Hideaki Ohno
 */
class HyperLogLog final {
  using byte_t = uint8_t;
  static constexpr double POW_2_32 = 4294967296.0;      ///< 2^32
  static constexpr double NEG_POW_2_32 = -4294967296.0; ///< -(2^32)

public:
  // b bit width (register size will be 2 to the b power).
  // This value must be in the range[4,30].
  // Default value is 16
  // error rate table
  //  K=10 | size=1024 | err=0.0423772
  //  K=11 | size=2048 | err=0.0213486
  //  K=12 | size=4096 | err=0.0196614
  //  K=13 | size=8192 | err=0.0155507
  //  K=14 | size=16384 | err=0.011504
  //  K=15 | size=32768 | err=0.00776367
  //  K=16 | size=65536 | err=0.00541403
  //  K=17 | size=131072 | err=0.00108688
  //  K=18 | size=262144 | err=0.00058702
  //  K=19 | size=524288 | err=0.000827101
  //  K=20 | size=1048576 | err=0.00082
  HyperLogLog(byte_t width = 15)
    : width_{ width },
      left_{ static_cast<byte_t>(32 - width) },
      size_{ static_cast<uint32_t>(1 << width) },
      data_(size_, 0),
      minIdx_{ size_ },
      maxIdx_{ 0 } {

    N_ENSURE(width_ >= 4 && width_ <= 30, "Invalid bit width value.");

    // compute alpha value
    double alpha;
    switch (size_) {
    case 16:
      alpha = 0.673;
      break;
    case 32:
      alpha = 0.697;
      break;
    case 64:
      alpha = 0.709;
      break;
    default:
      alpha = 0.7213 / (1.0 + 1.079 / size_);
      break;
    }
    alpha_ = alpha * size_ * size_;
  }

  ~HyperLogLog() = default;

public:
  static inline byte_t leadingZeros(uint32_t x, byte_t max = 32) {
#if defined(__has_builtin) && (defined(__GNUC__) || defined(__clang__))
    // __builtin_clz(0) is undefined
    if (UNLIKELY(x == 0)) {
      return max + 1;
    }
    return std::min<byte_t>(max, ::__builtin_clz(x)) + 1;
#else
    byte_t v = 1;
    while (v <= max && !(x & 0x80000000)) {
      v++;
      x <<= 1;
    }
    return v;
#endif
  }

  // interface to add single value
  template <typename T, typename std::enable_if_t<std::is_arithmetic_v<T>, int> = 0>
  inline void add(T v) {
    add(reinterpret_cast<const char*>(&v), sizeof(T));
  }

  // string_view type
  inline void add(const std::string_view& strv) {
    add(strv.data(), strv.size());
  }

  // add binary to the store
  void add(const char* data, size_t size) {

    // uint32_t hash = Murmur3::hash(data, size);
    uint32_t hash = (uint32_t)nebula::common::Hasher::hash64(data, size);
    uint32_t index = hash >> left_;
    byte_t rank = leadingZeros((hash << width_), left_);
    if (rank > data_[index]) {
      data_[index] = rank;

      // update the range that seeing values
      if (index > maxIdx_) {
        maxIdx_ = index;
      }

      if (index < minIdx_) {
        minIdx_ = index;
      }
    }
  }

  // Estimates cardinality value.
  double estimate() const {
    double sum = 0.0;
    uint32_t zeros = 0;
    for (uint32_t i = 0; i < size_; ++i) {
      auto v = data_[i];
      sum += 1.0 / (1 << v);
      if (v == 0) {
        ++zeros;
      }
    }

    double estimate = alpha_ / sum; // E in the original paper
    if (estimate <= 2.5 * size_) {
      if (zeros > 0) {
        estimate = size_ * log(static_cast<double>(size_) / zeros);
      }
    } else if (estimate > (1.0 / 30.0) * POW_2_32) {
      estimate = NEG_POW_2_32 * log(1.0 - (estimate / POW_2_32));
    }

    return estimate;
  }

  // Merges the estimate from 'other' into this object
  // TODO(cao): merge degrades accuracy fast when size is not big enough
  void merge(const HyperLogLog& other) {
    N_ENSURE(size_ == other.size_, "can't merge different sized hll.");
    for (uint32_t i = other.minIdx_; i < other.maxIdx_; ++i) {
      if (data_[i] < other.data_[i]) {
        data_[i] |= other.data_[i];
      }
    }

    // new range seeing values
    minIdx_ = std::min(minIdx_, other.minIdx_);
    maxIdx_ = std::max(maxIdx_, other.maxIdx_);
  }

  // Clears all internal registers.
  inline void clear() {
    std::fill(data_.begin(), data_.end(), 0);
    minIdx_ = size_;
    maxIdx_ = 0;
  }

  // Returns size of register.
  inline uint32_t size() const {
    return size_;
  }

  inline uint32_t minIdx() const {
    return minIdx_;
  }

  inline uint32_t maxIdx() const {
    return maxIdx_;
  }

  // Exchanges the content of the instance
  void swap(HyperLogLog& rhs) {
    std::swap(width_, rhs.width_);
    std::swap(left_, rhs.left_);
    std::swap(size_, rhs.size_);
    std::swap(alpha_, rhs.alpha_);
    std::swap(minIdx_, rhs.minIdx_);
    std::swap(maxIdx_, rhs.maxIdx_);
    std::swap(data_, rhs.data_);
  }

  // Dump the current status to a stream
  void serialize(std::ostream& os) {
    // width recording
    os.write((char*)&width_, sizeof(byte_t));
    os.write((char*)data_.data(), size_);
    N_ENSURE(!os.fail(), "Failed to serialize.");
  }

  // Restore the status from a stream
  void deserialize(std::istream& is) {
    byte_t width = 0;
    is.read((char*)&width, sizeof(byte_t));
    HyperLogLog tempHLL(width);
    is.read((char*)(tempHLL.data_.data()), tempHLL.size_);
    N_ENSURE(!is.fail(), "Failed to deserialize.");

    swap(tempHLL);
  }

public:
  // most of the time the matrix is pretty sparse
  // we want to compress it for storing.
  inline std::string_view data() const {
    // you should compress at least half
    return std::string_view((char*)data_.data(), size_);
  }

  // decompress the compressed binary into matrix
  inline void set(const std::string_view& sv, uint32_t minIdx, uint32_t maxIdx) {
    N_ENSURE_EQ(sv.size(), size_, "unexpected size of data to restore");
    std::memcpy((char*)data_.data(), sv.data(), sv.size());
    minIdx_ = minIdx;
    maxIdx_ = maxIdx;
  }

private:
  byte_t width_;
  byte_t left_;
  uint32_t size_;
  std::vector<byte_t> data_;

  double alpha_;

  // for optimization
  uint32_t minIdx_;
  uint32_t maxIdx_;
};

} // namespace common
} // namespace nebula