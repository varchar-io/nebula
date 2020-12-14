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
#include <fmt/format.h>
#include <folly/stats/Histogram.h>

#include "surface/eval/UDF.h"

namespace nebula {
namespace api {
namespace udf {

template <nebula::type::Kind IK,
          typename Traits = nebula::surface::eval::UdfTraits<nebula::surface::eval::UDFType::HIST, IK>,
          typename BaseType = nebula::surface::eval::UDAF<Traits::Type, IK>>
class Hist : public BaseType {
  // Set default number of buckets to 20
  static constexpr size_t BUCKET_NUM = 20;

public:
  using InputType = typename BaseType::InputType;
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

    inline virtual void merge(InputType v) override {
      histogram_.addValue(v);
    }

    inline virtual void mix(const nebula::surface::eval::Sketch& another) override {
      auto right = static_cast<const Aggregator&>(another);
      histogram_.merge(right.histogram_);
    }

    inline virtual NativeType finalize() override {
      json_ = jsonfy();
      return json_;
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      auto jsonStr = jsonfy();
      auto bin = slice.write(offset, jsonStr.size());
      auto size = slice.write(offset + bin, jsonStr.c_str(), jsonStr.size());
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
      for (rapidjson::Value::ConstMemberIterator itr = document.MemberBegin(); itr != document.MemberEnd(); ++itr) {
        int64_t idx = 0;
        uint64_t bucketCount = 0;
        InputType bucketSum = 0;
        for (auto& v : itr->value.GetArray()) {
          if (idx == 2) {
            bucketCount = std::stoi(v.GetString());
          } else if (idx == 3) {
            bucketSum = static_cast<InputType>(std::stod(v.GetString()));
          }
          idx++;
        }
        histogram_.addRepeatedValue(bucketSum / bucketCount, bucketCount);
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

    std::string jsonfy() const noexcept {
      // Save histogram in below json format:
      // {"bucketIndex": ["minVal", "maxVal", "count", "sum"]}
      rapidjson::StringBuffer buffer;
      buffer.Clear();
      rapidjson::Writer<rapidjson::StringBuffer> json(buffer);
      json.StartObject();
      for (size_t i = 0; i < histogram_.getNumBuckets(); ++i) {
        if (histogram_.getBucketByIndex(i).count == 0) {
          continue;
        }
        const auto& bucket = histogram_.getBucketByIndex(i);
        auto index = std::to_string(i);
        json.Key(index.data(), index.size());
        json.StartArray();
        std::ostringstream bucketMin;
        bucketMin << histogram_.getBucketMin(i);
        json.String(bucketMin.str().data(), bucketMin.str().size());
        std::ostringstream bucketMax;
        bucketMax << histogram_.getBucketMax(i);
        json.String(bucketMax.str().data(), bucketMax.str().size());
        std::ostringstream bucketCount;
        bucketCount << bucket.count;
        json.String(bucketCount.str().data(), bucketCount.str().size());
        std::ostringstream bucketSum;
        bucketSum << bucket.sum;
        json.String(bucketSum.str().data(), bucketSum.str().size());
        json.EndArray();
      }
      json.EndObject();
      return buffer.GetString();
    }
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
