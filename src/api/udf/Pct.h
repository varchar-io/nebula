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
 * Implement UDAF Pct to get quantiles of target values using folly/tdigest implementation
 * Internally this UDAF maintains a sketch binary stream with all numbers in double type.
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - percentile value through tdigest
template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::PCT, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, IK>>
class Pct : public BaseType {
  // most commonly for percentiles
  static constexpr size_t DIGEST_SIZE = 100;
  static constexpr size_t BUFFER_SIZE = 4 * DIGEST_SIZE;

public:
  using InputType = typename BaseType::InputType;
  using NativeType = typename BaseType::NativeType;
  using BaseAggregator = typename BaseType::BaseAggregator;

public:
  class Aggregator : public BaseAggregator {
  public:
    explicit Aggregator(double percentile)
      : percentile_{ percentile / DIGEST_SIZE },
        digest_{ DIGEST_SIZE },
        serde_{} {
      buffer_.reserve(BUFFER_SIZE);
    }
    virtual ~Aggregator() = default;
    // aggregate an value in
    inline virtual void merge(InputType v) override {
      if (LIKELY(buffer_.size() < BUFFER_SIZE)) {
        buffer_.emplace_back(double(v));
        return;
      }

      flush();
    }

    // aggregate another aggregator
    // TODO(cao): the current merge signature is strange requiring an array incuring copy
    // We may use our own version of TDigest implementation
    // with better interface to merge single item by pointer or reference
    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      auto right = static_cast<const Aggregator&>(another);

      // merge buffer
      auto rightBufferSize = right.buffer_.size();
      if (rightBufferSize > 0) {
        if (rightBufferSize > buffer_.size()) {
          std::swap(buffer_, right.buffer_);
        }

        for (auto v : right.buffer_) {
          buffer_.emplace_back(v);
        }

        right.buffer_.clear();
      }

      // only merge object only when it has value
      if (right.digest_.count() > 0) {
        digest_ = digest_.merge(std::array<folly::TDigest, 1>{ right.digest_ });
      }
    }

    inline virtual NativeType finalize() override {
      flush();
      return static_cast<NativeType>(digest_.estimateQuantile(percentile_));
    }

    std::string jsonfy() {
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
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      flush();

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
    inline virtual size_t load(nebula::common::ExtendableSlice& slice, size_t offset) override {
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
    inline void flush() {
      if (buffer_.size() > 0) {
        digest_ = digest_.merge(buffer_);
        buffer_.clear();
      }
    }

  private:
    double percentile_;
    std::vector<double> buffer_;
    folly::TDigest digest_;
    std::string serde_;
  };

public:
  Pct(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr, double percentile)
    : BaseType(name,
               std::move(expr),
               [p = percentile]() -> std::shared_ptr<Aggregator> {
                 return std::make_shared<Aggregator>(p);
               }) {}

  virtual ~Pct() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula