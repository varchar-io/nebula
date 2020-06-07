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

#include "common/IStream.h"
#include "common/StackTree.h"
#include "surface/eval/UDF.h"

/**
 * Implement UDAF TPM(Tree Path Merge) to merge tree paths into a weighted tree.
 * Every tree node will record its content and votes as weight.
 * 
 * The output of this aggregation can be usually visualized as flame graph or icicle graph (upside down).
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - merge tree binary (JSON) through merging tree path (call stack)
template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::TPM, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, IK>>
class Tpm : public BaseType {
public:
  using InputType = typename BaseType::InputType;
  using NativeType = typename BaseType::NativeType;
  using BaseAggregator = typename BaseType::BaseAggregator;

public:
  class Aggregator : public BaseAggregator {
    using STT = nebula::common::StackTree<std::string>;
    static constexpr auto SIZE_SIZE = sizeof(size_t);

  public:
    explicit Aggregator() : stack_{ std::make_unique<STT>() } {}
    virtual ~Aggregator() = default;

    // aggregate an value in
    inline virtual void merge(InputType v) override {
      // TODO(cao) - check unnecessary string values copy here
      // merge a list of value wrapped in a new line separated graph.
      nebula::common::IStream stream(v);
      stack_->merge(stream);
    }

    // aggregate another aggregator
    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      // merge another tree
      const auto& right = static_cast<const Aggregator&>(another);
      stack_->merge(*right.stack_);
    }

    inline virtual NativeType finalize() override {
      // get the final result - JSON blob
      json_ = stack_->jsonfy();
      return json_;
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      // write the merged tree into a memory chunk
      auto json = stack_->jsonfy();
      auto bin = slice.write(offset, json.size());
      auto size = slice.write(offset + bin, json.data(), json.size());
      return bin + size;
    }

    // deserialize from a given buffer, and bin size
    inline virtual size_t load(nebula::common::ExtendableSlice& slice, size_t offset) override {
      // load a merged tree from a memory chunk
      auto size = slice.read<size_t>(offset);

      // load the whole string_view and initialize current stack
      auto json = slice.read(offset + SIZE_SIZE, size);
      stack_ = std::make_unique<STT>(json);
      return size + SIZE_SIZE;
    }

    inline virtual bool fit(size_t) override {
      return false;
    }

  private:
    // TODO(cao): we need a way to figure out sub type of array
    std::unique_ptr<STT> stack_;
    std::string json_;
  };

public:
  Tpm(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr)
    : BaseType(name,
               std::move(expr),
               []() -> std::shared_ptr<Aggregator> {
                 return std::make_shared<Aggregator>();
               }) {}

  virtual ~Tpm() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula