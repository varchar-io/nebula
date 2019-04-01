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
#include "api/UDF.h"
#include "api/dsl/Expressions.h"
#include "common/Errors.h"
#include "glog/logging.h"
#include "meta/Table.h"
#include "type/Tree.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace udf {
// Used for demo purpose - revert the bool value
class Not : public UDF<nebula::type::Kind::BOOLEAN> {
public:
  Not(std::shared_ptr<nebula::api::dsl::Expression> expr) : expr_{ expr->asEval() }, colrefs_{ std::move(expr->columnRefs()) } {
  }
  virtual ~Not() = default;

public:
  // columns referenced
  virtual std::vector<std::string> columns() const override {
    return colrefs_;
  }

  // apply a row data to get result
  virtual NativeType eval(const nebula::surface::RowData& row) override {
    auto origin = (expr_->eval<bool>(row));
    LOG(INFO) << "origin=" << origin << ", reverse=" << !origin;
    return !origin;
  }

private:
  std::unique_ptr<nebula::execution::eval::ValueEval> expr_;
  std::vector<std::string> colrefs_;
};

} // namespace udf
} // namespace api
} // namespace nebula