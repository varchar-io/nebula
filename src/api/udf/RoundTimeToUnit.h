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

#include <fmt/format.h>
#include "common/Evidence.h"

#include "surface/eval/UDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {
// Round a time 
using UdfRoundBase = nebula::surface::eval::UDF<nebula::type::Kind::BIGINT, nebula::type::Kind::BIGINT>;
class RoundTimeToUnit : public UdfRoundBase {
    using TimeUnitType = typename nebula::type::TypeTraits<nebula::type::Kind::BIGINT>::CppType;
    using RoundedTimeType = typename nebula::type::TypeTraits<nebula::type::Kind::BIGINT>::CppType;
public:
  RoundTimeToUnit(const std::string& name, 
  std::unique_ptr<nebula::surface::eval::ValueEval> expr,
  TimeUnitType unit)
    : UdfRoundBase(
      name,
      std::move(expr),
      [unit](const std::optional<InputType>& origin) -> std::optional<int> {
        if (N_UNLIKELY(origin == std::nullopt)) {
          return std::nullopt;
        }

        static constexpr int32_t HOUR_CASE = 0;
        static constexpr int32_t DAY_CASE = 1;
        static constexpr int32_t WEEK_CASE = 2;
        static constexpr int32_t MONTH_CASE = 3;
        static constexpr int32_t QUARTER_CASE = 4;
        static constexpr int32_t YEAR_CASE = 5;

        // need to subtract begintime

        InputType timeSecs = origin.value();
        RoundedTimeType res = std::numeric_limits<RoundedTimeType>::lowest();
        std::time_t timePoint = (std::time_t) timeSecs;
        switch (unit) {
            case HOUR_CASE: {
              std::time_t roundedPoint = nebula::common::Evidence::hour(timePoint); 
              // LOG(INFO) << "FORMATTED ROUNDED POINT: " << nebula::common::Evidence::fmt_normal(roundedPoint);
              res = (RoundedTimeType) roundedPoint;
              break;
            }
            case DAY_CASE: {
              std::time_t roundedPoint = nebula::common::Evidence::date(timePoint); 
              // LOG(INFO) << "FORMATTED ROUNDED POINT: " << nebula::common::Evidence::fmt_normal(roundedPoint);
              res = (RoundedTimeType) roundedPoint;
              break;
            }
            case WEEK_CASE: { 
              std::time_t roundedPoint = nebula::common::Evidence::week(timePoint); 
              // LOG(INFO) << "FORMATTED ROUNDED POINT: " << nebula::common::Evidence::fmt_normal(roundedPoint);
              res = (RoundedTimeType) roundedPoint;
              break;
            }
            case MONTH_CASE: { 
                std::time_t roundedPoint = nebula::common::Evidence::month(timePoint); 
                // LOG(INFO) << "FORMATTED ROUNDED POINT: " << nebula::common::Evidence::fmt_normal(roundedPoint);
                res = (RoundedTimeType) roundedPoint;
                break;
            }
            case QUARTER_CASE: {
                break;
            }
            case YEAR_CASE: { 
                std::time_t roundedPoint = nebula::common::Evidence::year(timePoint); 
                // LOG(INFO) << "FORMATTED ROUNDED POINT: " << nebula::common::Evidence::fmt_normal(roundedPoint);
                res = (RoundedTimeType) roundedPoint;
                break;
            }
        }

        N_ENSURE_NE(res, std::numeric_limits<RoundedTimeType>::lowest(), "res should be defined");
        
        LOG(INFO) << "timeSecs: " << nebula::common::Evidence::fmt_normal(timeSecs);
        LOG(INFO) << "unit: " << unit;
        LOG(INFO) << "res: " << nebula::common::Evidence::fmt_normal(res);
        return res;
      }) {}
  virtual ~RoundTimeToUnit() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula