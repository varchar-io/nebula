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

#include "common/Likely.h"
#include "fmt/format.h"

namespace nebula {
namespace memory {
namespace serde {
/**
 * Define histogram data - Count is common value indicating total valid values.
 */
struct Histogram {
  Histogram() : count{ 0 } {}
  virtual ~Histogram() = default;

  virtual inline std::string toString() const {
    return fmt::format("{{ \"count\": {0} }}", count);
  }

  uint64_t count;
};

struct BoolHistogram : public Histogram {
  BoolHistogram() : trueValues{ 0 } {}
  virtual ~BoolHistogram() = default;

  virtual inline std::string toString() const override {
    return fmt::format("{{ \"count\": {0}, \"trues\": {1} }}", count, trueValues);
  }

  uint64_t trueValues;
};

template <typename T>
struct NumberHistogram : public Histogram {
  NumberHistogram() : v_min{ std::numeric_limits<T>::max() },
                      v_max{ std::numeric_limits<T>::min() },
                      v_sum{ 0 } {}
  virtual ~NumberHistogram() = default;

  inline T min() const {
    return v_min;
  }

  inline T max() const {
    return v_max;
  }

  inline T sum() const {
    return v_sum;
  }

  inline T avg() const {
    if (UNLIKELY(count == 0)) {
      return 0;
    }

    return v_sum / count;
  }

  virtual inline std::string toString() const override {
    return fmt::format("{{ \"count\": {0}, \"sum\": {1}, \"min\": {2}, \"max\": {3} }}",
                       count, v_sum, v_min, v_max);
  }

  T v_min;
  T v_max;
  T v_sum;
};

// cover all integers - we may have overflow issue for bigint
using IntHistogram = struct NumberHistogram<int64_t>;

// cover all floating values
using RealHistogram = struct NumberHistogram<double>;

} // namespace serde
} // namespace memory
} // namespace nebula