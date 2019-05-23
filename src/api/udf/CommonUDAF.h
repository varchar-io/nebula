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

// UDAF - a common pattern
template <nebula::type::Kind KIND>
class CommonUDAF : public nebula::execution::eval::UDAF<KIND> {
  using NativeType = typename nebula::type::TypeTraits<KIND>::CppType;
  using AggFunc = std::function<NativeType(NativeType, NativeType)>;

public:
  CommonUDAF(
    const std::string& name,
    std::unique_ptr<nebula::execution::eval::ValueEval> expr,
    AggFunc&& aggFunc,
    AggFunc&& partialFunc)
    : nebula::execution::eval::UDAF<KIND>(
        fmt::format("{0}({1})", name, expr->signature()),
        std::move(aggFunc),
        std::move(partialFunc)),
      expr_{ std::move(expr) } {}
  virtual ~CommonUDAF() = default;

public:
  // apply a row data to get result
  virtual NativeType run(nebula::execution::eval::EvalContext& ctx, bool& valid) const override {
    return expr_->eval<NativeType>(ctx, valid);
  }

  // global aggregate
  virtual void global(const nebula::surface::RowData&) override {
    throw NException("global agg exception");
  }

private:
  std::unique_ptr<nebula::execution::eval::ValueEval> expr_;
};

} // namespace udf
} // namespace api
} // namespace nebula