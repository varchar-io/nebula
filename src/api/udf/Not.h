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

#include <fmt/format.h>
#include "CommonUDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {
// Used for demo purpose - revert the bool value
using UdfNotBase = CommonUDF<nebula::type::Kind::BOOLEAN, nebula::type::Kind::BOOLEAN>;
class Not : public UdfNotBase {
public:
  using NativeType = nebula::type::TypeTraits<nebula::type::Kind::BOOLEAN>::CppType;
  Not(const std::string& name, std::unique_ptr<nebula::execution::eval::ValueEval> expr)
    : UdfNotBase(
        name,
        std::move(expr),
        [](const NativeType& origin, bool& valid) {
          if (!valid) {
            return true;
          }

          // otherwise reverse
          return !origin;
        }) {}
  virtual ~Not() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula