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
#include <fmt/format.h>
#include <folly/stats/Histogram.h>

#include "surface/eval/UDF.h"

namespace nebula {
namespace api {
namespace udf {

using nebula::type::TypeDetect;

template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::HIST, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, IK>>
class Hist : public BaseType {
  // Set default number of buckets to 10
  static constexpr size_t BUCKET_NUM = 10;

public:
  using InputType = typename std::conditional_t<std::is_floating_point_v<typename BaseType::InputType>, double, int64_t>;
  using NativeType = typename BaseType::NativeType;
  using BaseAggregator = typename BaseType::BaseAggregator;

public:
  class Aggregator : public BaseAggregator {
  static constexpr auto SIZE_SIZE = sizeof(size_t);

  public:
    explicit Aggregator(InputType min, InputType max, size_t bucketNum = BUCKET_NUM)
      : min_{ min },
      max_{ max },
      bucketNum_{ bucketNum },
      bucketSize_{ static_cast<InputType>((max_ - min_) / bucketNum) },
      histogram_{ bucketSize_, min_, max_ } {
    }
    virtual ~Aggregator() = default;

    inline virtual void merge(typename BaseType::InputType v) override {
      histogram_.addValue(v);
    }

    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      auto right = static_cast<const Aggregator&>(another);
      histogram_.merge(right.histogram_);
    }

    inline virtual NativeType finalize() override {
      json_ = jsonfy(true);
      return json_;
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      auto jsonStr = jsonfy(false);
      auto bin = slice.write(offset, jsonStr.size());
      auto size = slice.write(offset + bin, jsonStr.data(), jsonStr.size());
      return bin + size;
    }

    inline virtual size_t load(nebula::common::ExtendableSlice& slice, size_t offset) override {
      auto size = slice.read<size_t>(offset);
      auto jsonStr = std::string(slice.read(offset + SIZE_SIZE, size));
      histogram_ = folly::Histogram<InputType>(bucketSize_, min_, max_);
      rapidjson::Document document;
      if (document.Parse(jsonStr.c_str()).HasParseError()) {
        throw NException(fmt::format("Error parsing histogram json: {0}", jsonStr));
      }
      for (auto& bucket : document["b"].GetArray()) {
        int64_t idx = 0;
        uint64_t bucketCount = 0;
        InputType bucketSum = 0;
        for (auto& v : bucket.GetArray()) {
          if (idx == 2) {
            bucketCount = v.GetInt64();
          } else if (idx == 3) {
            bucketSum = v.GetDouble();
          }
          idx++;
        }
        if (bucketCount != 0) {
          histogram_.addRepeatedValue(bucketSum / bucketCount, bucketCount);
        }
      }

      return size + SIZE_SIZE;
    }

    inline virtual bool fit(size_t) override {
      return false;
    }

  private:
    InputType min_;
    InputType max_;
    size_t bucketNum_;
    InputType bucketSize_;
    folly::Histogram<InputType> histogram_;
    std::string json_;

#define SAVE_VALUE(value) \
  if constexpr (std::is_floating_point_v<InputType>) {    \
    json.Double(value);                                        \
  } else {                                                     \
    json.Int64(value);                                         \
  }
    std::string jsonfy(bool finalize = false) const noexcept {
      // Save histogram in below json format:
      // {"b": [[minVal1, maxVal1, count1, sum1],[minVal2, maxVal2, count2, sum2]...]}
      // The size of the json array is the total number of buckets in histogram
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> json(buffer);
      json.StartObject();
      json.Key("b");
      json.StartArray();
      auto numBuckets = histogram_.getNumBuckets();
      for (size_t i = 0; i < numBuckets; ++i) {
        const auto& bucket = histogram_.getBucketByIndex(i);
        if (finalize && i == numBuckets - 2) {
          if (histogram_.getBucketMax(i) > histogram_.getBucketMin(i + 1)) {
            const auto& lastBucket = histogram_.getBucketByIndex(i + 1);
            json.StartArray();
            SAVE_VALUE(histogram_.getBucketMin(i));
            SAVE_VALUE(histogram_.getBucketMax(i + 1));
            json.Int64(bucket.count + lastBucket.count);
            json.Double(bucket.sum + lastBucket.sum);
            json.EndArray();
            break;
          }
        }
        json.StartArray();
        SAVE_VALUE(histogram_.getBucketMin(i));
        SAVE_VALUE(histogram_.getBucketMax(i));
        json.Int64(bucket.count);
        json.Double(bucket.sum);
        json.EndArray();
      }
      json.EndArray();
      json.EndObject();
      return buffer.GetString();
    }
#undef SAVE_VALUE
  };
public:
  Hist(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr, InputType min, InputType max)
    : BaseType(name,
               std::move(expr),
               [min = min, max = max]() -> std::shared_ptr<Aggregator> {
                 return std::make_shared<Aggregator>(min, max);
               }) {}

  virtual ~Hist() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula
