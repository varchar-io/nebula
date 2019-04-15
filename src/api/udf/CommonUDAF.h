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
#include <array>
#include "api/dsl/Expressions.h"
#include "common/Errors.h"
#include "common/Likely.h"
#include "execution/eval/UDF.h"
#include "glog/logging.h"
#include "meta/Table.h"
#include "type/Tree.h"

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
  CommonUDAF(std::shared_ptr<nebula::api::dsl::Expression> expr, AggFunc&& aggFunc)
    : expr_{ expr->asEval() },
      colrefs_{ std::move(expr->columnRefs()) },
      nebula::execution::eval::UDAF<KIND>(std::move(aggFunc)) {}
  virtual ~CommonUDAF() = default;

public:
  // columns referenced
  virtual std::vector<std::string> columns() const override {
    return colrefs_;
  }

  // apply a row data to get result
  virtual NativeType run(const nebula::surface::RowData& row) const override {
    return expr_->eval<NativeType>(row);
  }

  // partial aggregate
  virtual void partial(const nebula::surface::RowData&) override {
    throw NException("partial agg exception");
  }

  // global aggregate
  virtual void global(const nebula::surface::RowData&) override {
    throw NException("global agg exception");
  }

private:
  std::unique_ptr<nebula::execution::eval::ValueEval> expr_;
  std::vector<std::string> colrefs_;
};

} // namespace udf
} // namespace api
} // namespace nebula