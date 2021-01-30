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

#include <atomic>

#include "common/Memory.h"
#include "surface/eval/UDF.h"

/**
 * Implement UDAF AVG, we will need both count_ and sum_ data maintained internally.
 * There are two ways to implement it 
 * 1) function level: maintain states inside the function itself. 
 * 2) execution level: modify schema by expanding avg(c) => {sum_(c), count_(c)} and do the division in global aggregation. 
 * 
 * The first approach is cleaner, but may need special handling to complicate UDF family.
 * The second approach requires execution engine to do special case handling.
 * 
 */
namespace nebula {
namespace api {
namespace udf {

template <typename T>
inline auto extract(const int128_t& v) ->
  typename std::enable_if_t<std::is_integral_v<T>, int64_t> {
  return nebula::common::Int128_U::high64<int64_t>(v);
}

template <typename T>
inline auto extract(const int128_t& v) ->
  typename std::enable_if_t<std::is_floating_point_v<T>, double> {
  return nebula::common::Int128_U::high64<double>(v);
}

// UDAF - avg
template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::AVG, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, IK>>
class Avg : public BaseType {
public:
  using InputType = typename BaseType::InputType;
  using NativeType = typename BaseType::NativeType;
  using StoreType = typename std::conditional_t<std::is_floating_point_v<InputType>, double,
                                                std::conditional_t<std::is_same_v<InputType, int128_t>, int128_t, int64_t>>;
  using BaseAggregator = typename BaseType::BaseAggregator;

public:
  class Aggregator : public BaseAggregator {
    static constexpr auto StoreSize = sizeof(StoreType) + 4;

  public:
    Aggregator() : sum_{ 0 }, count_{ 0 } {}
    virtual ~Aggregator() = default;

    // aggregate an value in
    inline virtual void merge(InputType v) override {
      sum_ += v;
      ++count_;
    }

    // aggregate another aggregator
    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      auto v2 = static_cast<const Aggregator&>(another);
      // merge sum_
      sum_ += v2.sum_;

      // merge count_
      count_ += v2.count_;
    }

    inline virtual NativeType finalize() override {
      if (count_ > 0) {
        return static_cast<NativeType>(sum_ / count_);
      }
      return 0;
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      offset += slice.write(offset, sum_);
      slice.write(offset, count_);
      return StoreSize;
    }

    // deserialize from a given buffer, and bin size
    inline virtual size_t load(nebula::common::ExtendableSlice& slice, size_t offset) override {
      sum_ = slice.read<StoreType>(offset);
      count_ = slice.read<int32_t>(offset + sizeof(StoreType));
      return StoreSize;
    }

    inline virtual bool fit(size_t space) override {
      return space >= StoreSize;
    }

  private:
    StoreType sum_;
    int32_t count_;
  };

public:
  Avg(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr)
    : BaseType(name,
               std::move(expr),
               []() -> std::shared_ptr<Aggregator> {
                 return std::make_shared<Aggregator>();
               }) {}

  virtual ~Avg() = default;
};

template <>
Avg<nebula::type::Kind::INVALID>::Avg(
  const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr);

template <>
Avg<nebula::type::Kind::BOOLEAN>::Avg(
  const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr);

template <>
Avg<nebula::type::Kind::VARCHAR>::Avg(
  const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr);

} // namespace udf
} // namespace api
} // namespace nebula