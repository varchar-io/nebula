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

#include "common/HyperLogLog.h"
#include "surface/eval/UDF.h"

/**
 * Define cardinality aggregation function.
 * Supporting both 
 * 1. Estimated cardinality which is based off HyperLogLog.
 * 2. Precise cardinality which is based off hash map.
 * 
 * #2 is unbounded, the storage cost is linear to the value of cardinality, 
 * in extreme case, it may take all compute resources, so we will define a supported MAX.
 * 
 * #1 is bounded with error rate in consider, default settings work for major scenarios. 
 * support user customization for precision adjust.
 * 
 * In the version 1 implementation, it only supports #1.
 */
namespace nebula {
namespace api {
namespace udf {

// UDF-CARD: cardinality UDAF
// paramter of estimated or precise leading to different internal impl
template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::CARD, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, IK>>
class Cardinality : public BaseType {
public:
  using InputType = typename BaseType::InputType;
  using NativeType = typename BaseType::NativeType;
  using BaseAggregator = typename BaseType::BaseAggregator;

public:
  class Aggregator : public BaseAggregator {
    // 20 -> 1M (pow(2, 20)) buffer
    // good for cardinality around 1M numbers, otherwise merge degrades accruacy
    static constexpr auto LOG_SIZE = 20;

  public:
    explicit Aggregator(bool est, size_t logSize = LOG_SIZE)
      : est_{ est },
        log_{ std::make_unique<nebula::common::HyperLogLog>(logSize) } {
      N_ENSURE(est, "supporting estimated cardinality only for now.");
    }
    virtual ~Aggregator() = default;

    // aggregate an value in
    inline virtual void merge(InputType v) override {
      // get bytes of the input type value
      log_->add(v);
    }

    // aggregate another aggregator
    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      // merge another tree
      const auto& right = static_cast<const Aggregator&>(another);
      log_->merge(*right.log_);
    }

    inline virtual NativeType finalize() override {
      // get the final result - JSON blob
      // apply threshold in finalized result if present
      return (NativeType)log_->estimate();
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      uint32_t size = log_->size();
      auto head = slice.write(offset, size);
      auto body = slice.write(offset + head, log_->data(), size);
      return head + body;
    }

    // deserialize from a given buffer, and bin size
    inline virtual size_t load(nebula::common::ExtendableSlice& slice, size_t offset) override {
      static constexpr auto SIZE_SIZE = sizeof(uint32_t);
      auto size = slice.read<uint32_t>(offset);

      // here incurs a copy from string_view to string object
      auto bytes = slice.read(offset + SIZE_SIZE, size);
      log_->restore(bytes);
      return size + SIZE_SIZE;
    }

    inline virtual bool fit(size_t) override {
      return false;
    }

  private:
    size_t est_;
    std::unique_ptr<nebula::common::HyperLogLog> log_;
  };

public:
  Cardinality(
    const std::string& name,
    std::unique_ptr<nebula::surface::eval::ValueEval> expr,
    bool est = true)
    : BaseType(name,
               std::move(expr),
               [est]() -> std::shared_ptr<Aggregator> {
                 return std::make_shared<Aggregator>(est);
               }) {}

  virtual ~Cardinality() = default;
};
} // namespace udf
} // namespace api
} // namespace nebula