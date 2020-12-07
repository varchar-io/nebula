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

#include "surface/eval/UDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {
// Used for demo purpose - revert the bool value
using UdfNotBase = nebula::surface::eval::UDF<nebula::type::Kind::BOOLEAN, nebula::type::Kind::BOOLEAN>;
class Not : public UdfNotBase {
public:
  Not(const std::string& name, std::unique_ptr<nebula::surface::eval::ValueEval> expr)
    : UdfNotBase(
      name,
      std::move(expr),
      [](const std::optional<NativeType>& origin) {
        if (origin == std::nullopt) {
          return true;
        }

        // otherwise reverse
        return !origin.value();
      }) {}
  virtual ~Not() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula