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

#include <atomic>
#include <folly/stats/TDigest.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "surface/eval/UDF.h"

/**
 * Implement UDAF TDigest to get quantiles of target values
 * Internally this UDAF maintains a sketch binary stream with all numbers in double type.
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - TDigest
template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::TDIGEST, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, IK>>
class TDigest : public BaseType {
  // most commonly for percentiles
  static constexpr size_t DIGEST_SIZE = 100;

public:
  using InputType = typename BaseType::InputType;
  using NativeType = typename BaseType::NativeType;
  using BaseAggregator = typename BaseType::BaseAggregator;

public:
  class Aggregator : public BaseAggregator {
  public:
    virtual ~Aggregator() = default;
    // aggregate an value in
    inline virtual void merge(InputType v) override {
      digest_.merge(std::array<double, 1>{ static_cast<double>(v) });
    }

    // aggregate another aggregator
    // TODO(cao): the current merge signature is strange requiring an array incuring copy
    // We may use our own version of TDigest implementation
    // with better interface to merge single item by pointer or reference
    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      const auto& v2 = static_cast<const Aggregator&>(another).digest_;
      digest_.merge(std::array<folly::TDigest, 1>{ v2 });
    }

    inline virtual NativeType finalize() override {
      // set up JSON writer to serialize each row
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> json(buffer);
      json.StartObject();
#define KV(V, NAME) \
  json.Key(#NAME);  \
  json.Double(V.NAME());

      KV(digest_, sum)
      KV(digest_, count)
      KV(digest_, max)
      KV(digest_, min)
      KV(digest_, maxSize)

      json.Key("centroids");
      json.StartArray();
      const auto& centroids = digest_.getCentroids();
      for (const auto& c : centroids) {
        json.StartObject();
        KV(c, mean)
        KV(c, weight)
        json.EndObject();
      }
      json.EndArray();
#undef KV
      json.EndObject();

      // TODO(cao) - thinking how to avoid dangling string view
      // if the aggregator destructed before client usage - a bad case for using string_view
      serde_ = std::string(buffer.GetString());
      return serde_;
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::PagedSlice& slice, size_t offset) const override {
      // record first offset - final size will be delta of offset
      size_t origin = offset;
      // write sum
      offset += slice.write(offset, digest_.sum());
      // write count
      offset += slice.write(offset, digest_.count());
      // write max
      offset += slice.write(offset, digest_.max());
      // write min
      offset += slice.write(offset, digest_.min());
      // write max size
      offset += slice.write(offset, digest_.maxSize());
      // write all centroids
      const auto& centroids = digest_.getCentroids();
      // write count of centroids
      offset += slice.write(offset, centroids.size());
      for (const auto& c : centroids) {
        offset += slice.write(offset, c.mean());
        offset += slice.write(offset, c.weight());
      }

      return offset - origin;
    }

    // deserialize from a given buffer, and bin size
    inline virtual size_t load(nebula::common::PagedSlice& slice, size_t offset) override {
      static constexpr auto WIDTH = sizeof(double);

      size_t origin = offset;
#define READ(NAME)                          \
  double NAME = slice.read<double>(offset); \
  offset += WIDTH;

      READ(sum)
      READ(count)
      READ(max)
      READ(min)

      size_t maxSize = slice.read<size_t>(offset);
      offset += WIDTH;
      size_t numCentroids = slice.read<size_t>(offset);
      offset += WIDTH;
      std::vector<folly::TDigest::Centroid> centroids;
      centroids.reserve(numCentroids);
      for (size_t i = 0; i < numCentroids; ++i) {
        READ(mean)
        READ(weight)
        centroids.emplace_back(mean, weight);
      }
#undef READ
      digest_ = folly::TDigest(std::move(centroids), sum, count, max, min, maxSize);
      return offset - origin;
    }

    inline virtual bool fit(size_t) override {
      return false;
    }

  private:
    std::string serde_;
    folly::TDigest digest_;
  };

public:
  TDigest(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr)
    : BaseType(name,
               std::move(expr),
               []() -> std::shared_ptr<Aggregator> {
                 return std::make_shared<Aggregator>();
               }) {}

  virtual ~TDigest() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula