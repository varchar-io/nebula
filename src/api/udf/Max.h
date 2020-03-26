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

#include <fmt/format.h>

#include "surface/eval/UDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - max
template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::MAX, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, IK>>
class Max : public BaseType {
public:
  using InputType = typename BaseType::InputType;
  using NativeType = typename BaseType::NativeType;
  using BaseAggregator = typename BaseType::BaseAggregator;

public:
  class Aggregator : public BaseAggregator {
    static constexpr auto StoreSize = sizeof(NativeType);

  public:
    Aggregator() : value_{ std::numeric_limits<NativeType>::min() } {}
    virtual ~Aggregator() = default;

    // aggregate an value in
    inline virtual void merge(InputType v) override {
      value_ = std::max<NativeType>(value_, v);
    }

    // aggregate another aggregator
    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      auto v2 = static_cast<const Aggregator&>(another).value_;
      value_ = std::max<NativeType>(value_, v2);
    }

    inline virtual NativeType finalize() override {
      return value_;
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      return slice.write(offset, value_);
    }

    // deserialize from a given buffer, and bin size
    inline virtual size_t load(nebula::common::ExtendableSlice& slice, size_t offset) override {
      value_ = slice.read<NativeType>(offset);
      return StoreSize;
    }

    inline virtual bool fit(size_t space) override {
      return space >= StoreSize;
    }

  private:
    NativeType value_;
  };

public:
  Max(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr)
    : BaseType(name,
               std::move(expr),
               []() -> std::shared_ptr<Aggregator> {
                 return std::make_shared<Aggregator>();
               }) {}
  virtual ~Max() = default;
};

template <>
class Max<nebula::type::Kind::VARCHAR> : public nebula::surface::eval::UDAF<nebula::type::Kind::VARCHAR, nebula::type::Kind::VARCHAR> {
public:
  Max(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr)
    : nebula::surface::eval::UDAF<nebula::type::Kind::VARCHAR, nebula::type::Kind::VARCHAR>(
        name,
        std::move(expr),
        {}) {
    throw NException("Not supporting max(varchar) yet.");
  }
  virtual ~Max() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula