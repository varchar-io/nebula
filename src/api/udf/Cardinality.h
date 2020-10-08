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

#include <lz4.h>

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
 *
 * HLL in chosen precision might be large (default log-size 20 implies 1MB store for each sketch).
 * In a group by aggregation scenario, we may have thousands of this for each key.
 * This imposes a risk for heavy memory usage exhausting the resources.
 * There are two options to address this concern:
 * 1. when working on local node, we can use auto-growing hash set to replace it,
 *    unless the set outgrows the store size (1MB). This makes HLL used only when it's needed.
 * 2. we reserve by the log-size because we want to support some level of scale, but it's not flexible
 *    so we should looking for algo to make it dynamic grow instead of allocating fixed memory.
 * 3. the major concern is to serialize data across boundary, we should use compress to reduce its size.
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
    static constexpr auto LOG_SIZE = 16;

  public:
    explicit Aggregator(bool est, uint32_t logSize = LOG_SIZE)
      : est_{ est },
        logSize_{ logSize },
        log_{ std::make_unique<nebula::common::HyperLogLog>(logSize_) } {
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
      return (NativeType)log_->estimate();
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      auto data = log_->data();
      auto src = data.data();
      size_t size = data.size();

      // wish half sized compression
      std::vector<char> buffer(size / 2, 0);
      size_t bytes = LZ4_compress_default(src, buffer.data(), size, size / 2);
      VLOG(1) << "Compressed HLL from " << size
              << " to " << bytes
              << ", min=" << log_->minIdx()
              << ", max=" << log_->maxIdx();
      if (bytes > 0) {
        src = buffer.data();
        size = bytes;
      }

      const auto origin = offset;

      offset += slice.write(offset, bytes > 0);
      offset += slice.write(offset, size);
      offset += slice.write(offset, src, size);
      offset += slice.write(offset, log_->minIdx());
      offset += slice.write(offset, log_->maxIdx());
      return offset - origin;
    }

    // deserialize from a given buffer, and bin size
    inline virtual size_t load(nebula::common::ExtendableSlice& slice, size_t offset) override {
      const auto origin = offset;
      auto flag = slice.read<bool>(offset);
      offset += sizeof(flag);
      auto size = slice.read<size_t>(offset);
      offset += sizeof(size);

      // here incurs a copy from string_view to string object
      auto bytes = slice.read(offset, size);
      offset += size;

      // read min/max id
      auto minIdx = slice.read<uint32_t>(offset);
      offset += sizeof(minIdx);
      auto maxIdx = slice.read<uint32_t>(offset);
      offset += sizeof(maxIdx);

      // if is compressed, we decompress it
      if (flag) {
        const auto raw = log_->size();
        std::vector<char> buffer(raw, 0);
        auto ret = (uint32_t)LZ4_decompress_safe(bytes.data(), buffer.data(), size, raw);
        N_ENSURE_EQ(ret, raw, "raw data size mismatches.");
        std::string_view bin(buffer.data(), raw);
        log_->set(bin, minIdx, maxIdx);
      } else {
        log_->set(bytes, minIdx, maxIdx);
      }

      return offset - origin;
    }

    inline virtual bool fit(size_t) override {
      return false;
    }

  private:
    bool est_;
    uint32_t logSize_;

    // mutual exclusive - converted when needed
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