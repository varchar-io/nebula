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

#include <algorithm>
#include "api/dsl/Base.h"
#include "execution/eval/UDF.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {

// UDAF - a common pattern for most UDAF which has its storage type same as native type
template <nebula::type::Kind KIND>
class CommonUDAF : public nebula::execution::eval::UDAF<KIND> {
public:
  using UDAFType = typename nebula::execution::eval::UDAF<KIND>;
  using NativeType = typename UDAFType::NativeType;
  using MergeFunction = typename UDAFType::MergeFunction;
  CommonUDAF(
    const std::string& name,
    std::unique_ptr<nebula::execution::eval::ValueEval> expr,
    MergeFunction&& mf)
    : nebula::execution::eval::UDAF<KIND>(
        name,
        std::move(expr),
        {},
        std::move(mf),
        {}) {}
  virtual ~CommonUDAF() = default;
};

} // namespace udf
} // namespace api
} // namespace nebula