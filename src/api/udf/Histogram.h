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
#include <glog/logging.h>
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
      std::ostringstream tsv;
      histogram_.toTSV(tsv);
      return static_cast<NativeType>(tsv.str());
    }

    // serialize into a buffer
    inline virtual size_t serialize(nebula::common::ExtendableSlice& slice, size_t offset) override {
      std::ostringstream tsv;
      histogram_.toTSV(tsv);
      auto tsvStr = tsv.str();
      auto bin = slice.write(offset, tsvStr.size());
      auto size = slice.write(offset + bin, tsvStr.c_str(), tsvStr.size());
      return bin + size;
    }

    inline virtual size_t load(nebula::common::ExtendableSlice& slice, size_t offset) override {
      auto size = slice.read<size_t>(offset);
      auto tsvStr = std::string(slice.read(offset + SIZE_SIZE, size));
      histogram_ = folly::Histogram<InputType>(bucketSize_, min_, max_);
      // reconstruct histgoram, split by '/n' to get buckets,
      // then split by '/t' to get min, max, count and sum values
      auto bucketStrs = split(tsvStr, "\n");
      for (auto bucketStr : bucketStrs) {
        if (bucketStr.empty()) {
          continue;
        }
        auto bucketVal = split(bucketStr, "\t");
        int64_t idx = 0;
        uint64_t bucketCount = 0;
        InputType bucketSum = 0;
        for (auto bv : bucketVal) {
          if (idx == 2) {
            bucketCount = std::stoi(bv);
          } else if (idx == 3) {
            bucketSum = static_cast<InputType>(std::stod(bv));
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

    // Helper method used to re-construct histogram
    std::vector<std::string> split(std::string s, std::string delimiter) {
      size_t pos_start = 0, pos_end, delim_len = delimiter.length();
      std::string token;
      std::vector<std::string> res;

      while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
        token = s.substr(pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        res.push_back(token);
      }

      res.push_back(s.substr(pos_start));
      return res;
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
