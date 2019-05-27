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

#include "CommonUDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {
/**
 * This UDF provides logic operations to determine if a value is in given set.
 */
template <nebula::type::Kind KIND>
class In : public CommonUDF<nebula::type::Kind::BOOLEAN, KIND> {
  using UdfInBase = CommonUDF<nebula::type::Kind::BOOLEAN, KIND>;
  using ExprType = typename nebula::type::TypeTraits<KIND>::CppType;
  using ValueType = typename std::conditional<
    KIND == nebula::type::Kind::VARCHAR,
    std::string,
    typename nebula::type::TypeTraits<KIND>::CppType>::type;

public:
  In(const std::string& name,
     std::unique_ptr<nebula::execution::eval::ValueEval> expr,
     const std::vector<ValueType>& values)
    : UdfInBase(
        name,
        std::move(expr),
        // logic for "in []"
        [this](const ExprType& source, bool& valid) -> bool {
          if (valid) {
            return std::any_of(values_.cbegin(), values_.cend(), [&source](const ValueType& v) {
              return source == v;
            });
          }

          return false;
        }),
      values_{ values } {}

  In(const std::string& name,
     std::unique_ptr<nebula::execution::eval::ValueEval> expr,
     const std::vector<ValueType>& values,
     bool in)
    : UdfInBase(name,
                std::move(expr),
                // logic for "not in []"
                [this](const ExprType& source, bool& valid) -> bool {
                  if (valid) {
                    return std::none_of(values_.cbegin(), values_.cend(), [&source](const ValueType& v) {
                      return source == v;
                    });
                  }

                  return false;
                }),
      values_{ values } {
    N_ENSURE(!in, "this constructor is designed for NOT IN clauase");
  }

  virtual ~In() = default;

private:
  const std::vector<ValueType> values_;
};

} // namespace udf
} // namespace api
} // namespace nebula