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

#include "surface/eval/UDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - count, the input is the constant 1 which is an integer value_ always
template <nebula::type::Kind IK = nebula::type::Kind::INTEGER,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::COUNT, nebula::type::Kind::INTEGER>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, nebula::type::Kind::INTEGER>>
class Count : public BaseType {
public:
  using InputType = typename BaseType::InputType;
  using NativeType = typename BaseType::NativeType;
  using BaseAggregator = typename BaseType::BaseAggregator;

public:
  class Aggregator : public BaseAggregator {
    static constexpr auto StoreSize = sizeof(NativeType);

  public:
    Aggregator() : value_{ 0 } {}
    virtual ~Aggregator() = default;

    // aggregate an value_ in
    inline virtual void merge(InputType) override {
      // regardless what input type is, always increment 1
      ++value_;
    }

    // aggregate another aggregator
    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      auto v2 = static_cast<const Aggregator&>(another).value_;
      value_ += v2;
    }

    inline virtual NativeType finalize() override {
      return value_;
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::PagedSlice& slice, size_t offset) override {
      return slice.write(offset, value_);
    }

    // deserialize from a given buffer, and bin size
    inline virtual size_t load(nebula::common::PagedSlice& slice, size_t offset) override {
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
  // for count, we don't need evaluate inner expr
  // unless it's distinct a column, so we can safely replace it with a const expression with value_ 0
  Count(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval>)
    : BaseType(name,
               nebula::surface::eval::constant(1),
               []() -> std::shared_ptr<Aggregator> {
                 return std::make_shared<Aggregator>();
               }) {}
  virtual ~Count() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula