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

#include <vector>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "common/Likely.h"
#include "fmt/format.h"

namespace nebula {
namespace surface {
namespace eval {

/**
 * Define histogram data - Count is common value indicating total valid values.
 */

#define TOSTRING_JSON_START(TYPE)                          \
  rapidjson::StringBuffer buffer;                          \
  rapidjson::Writer<rapidjson::StringBuffer> json(buffer); \
  json.StartObject();                                      \
  json.Key("type");                                        \
  json.String(TYPE);

#define TOSTRING_JSON_END \
  json.EndObject();       \
  return buffer.GetString();

struct Histogram {
  Histogram() : Histogram(0) {}
  Histogram(uint64_t c) : count{ c } {}
  virtual ~Histogram() = default;

  virtual inline std::string toString() const {
    TOSTRING_JSON_START("BASE")
    json.Key("count");
    json.Uint64(count);

    TOSTRING_JSON_END
  }

  virtual inline void merge(const Histogram& other) {
    count += other.count;
  }

  uint64_t count;
};

struct BoolHistogram : public Histogram {
  BoolHistogram() : BoolHistogram(0, 0) {}
  BoolHistogram(uint64_t c, uint64_t tv) : Histogram(c), trueValues{ tv } {}
  virtual ~BoolHistogram() = default;

  virtual inline std::string toString() const override {
    TOSTRING_JSON_START("BOOL")
    json.Key("count");
    json.Uint64(count);
    json.Key("trues");
    json.Uint64(trueValues);
    TOSTRING_JSON_END
  }

  virtual inline void merge(const Histogram& other) override {
    Histogram::merge(other);

    auto bh = static_cast<const BoolHistogram&>(other);
    trueValues += bh.trueValues;
  }

  uint64_t trueValues;
};

template <typename T>
struct NumberHistogram : public Histogram {
  NumberHistogram()
    : NumberHistogram<T>(0,
                         std::numeric_limits<T>::max(),
                         std::numeric_limits<T>::min(),
                         0) {}
  NumberHistogram(uint64_t c, T min, T max, T sum)
    : Histogram(c),
      v_min{ min },
      v_max{ max },
      v_sum{ sum } {}
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
#define TYPED_KV(TN, TM)   \
  TOSTRING_JSON_START(#TN) \
  json.Key("count");       \
  json.Uint64(count);      \
  json.Key("min");         \
  json.TM(v_min);          \
  json.Key("max");         \
  json.TM(v_max);          \
  json.Key("sum");         \
  json.TM(v_sum);

    if constexpr (std::is_floating_point_v<T>) {
      TYPED_KV(REAL, Double)
      TOSTRING_JSON_END
    } else {
      TYPED_KV(INT, Int64)
      TOSTRING_JSON_END
    }

#undef TYPED_KV
  }

  virtual inline void merge(const Histogram& other) override {
    Histogram::merge(other);

    auto nh = static_cast<const NumberHistogram<T>&>(other);
    v_min = std::min<T>(v_min, nh.v_min);
    v_max = std::max<T>(v_max, nh.v_max);
    v_sum += nh.v_sum;
  }

  T v_min;
  T v_max;
  T v_sum;
};

// cover all integers - we may have overflow issue for bigint
using IntHistogram = struct NumberHistogram<int64_t>;

// cover all floating values
using RealHistogram = struct NumberHistogram<double>;

#undef TOSTRING_JSON_END
#undef TOSTRING_JSON_START

// get histogram object from its serialzied string value
std::shared_ptr<Histogram> from(const std::string&);

// define histogram vector type for give table with fixed schema
using HistVector = std::vector<std::shared_ptr<nebula::surface::eval::Histogram>>;

} // namespace eval
} // namespace surface
} // namespace nebula