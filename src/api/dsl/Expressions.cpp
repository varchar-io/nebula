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

#include "Expressions.h"

namespace nebula {
namespace api {
namespace dsl {

using nebula::meta::Table;
using nebula::surface::eval::column;
using nebula::surface::eval::ValueEval;
using nebula::type::Kind;
using nebula::type::TreeBase;
using nebula::type::TreeNode;
using nebula::type::TypeBase;
using nebula::type::TypeNode;

//////////////////////////////////////// Column Expression Impl ///////////////////////////////////
// for column expression
#define LOGICAL_OP_STRING(OP, TYPE)                                                                                                                    \
  auto ColumnExpression::operator OP(const std::string_view value)->LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<std::string_view>> { \
    return LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<std::string_view>>(                                                           \
      std::make_shared<THIS_TYPE>(*this), std::make_shared<ConstExpression<std::string_view>>(value));                                                 \
  }

LOGICAL_OP_STRING(==, EQ)
LOGICAL_OP_STRING(>, GT)
LOGICAL_OP_STRING(>=, GE)
LOGICAL_OP_STRING(<, LT)
LOGICAL_OP_STRING(<=, LE)

#undef LOGICAL_OP_STRING

TypeInfo ColumnExpression::type(const Table& table) {
  // look up the table schema to deduce the table
  const auto& schema = table.schema();
  TreeNode nodeType;
  schema->onChild(column_, [&nodeType](const TypeNode& found) {
    nodeType = std::dynamic_pointer_cast<TreeBase>(found);
  });

  N_ENSURE_NOT_NULL(nodeType, fmt::format("column not found: {0}", column_));
  type_ = TypeInfo{ TypeBase::k(nodeType) };
  return type_;
}

// convert to value eval
#define KIND_CASE_VE(KIND, Type)  \
  case Kind::KIND: {              \
    return column<Type>(column_); \
  }

std::unique_ptr<ValueEval> ColumnExpression::asEval() const {
  auto k = typeInfo().native;
  if (UNLIKELY(k == Kind::INVALID)) {
    throw NException("Please call type() first to evalue the schema before convert to value eval tree");
  }

  switch (k) {
    KIND_CASE_VE(BOOLEAN, bool)
    KIND_CASE_VE(TINYINT, int8_t)
    KIND_CASE_VE(SMALLINT, int16_t)
    KIND_CASE_VE(INTEGER, int32_t)
    KIND_CASE_VE(BIGINT, int64_t)
    KIND_CASE_VE(REAL, float)
    KIND_CASE_VE(DOUBLE, double)
    KIND_CASE_VE(INT128, int128_t)
    KIND_CASE_VE(VARCHAR, std::string_view)
  default:
    // TODO(cao) - support list and map type for UDF to compute values on
    throw NException(fmt::format(
      "Not supported type {0} found in column expression", TypeBase::kname(k)));
  }
}

#undef KIND_CASE_VE

std::unique_ptr<ExpressionData> ColumnExpression::serialize() const noexcept {
  auto data = Expression::serialize();
  data->type = ExpressionType::COLUMN;
  data->c_name = column_;
  return data;
}

} // namespace dsl
} // namespace api
} // namespace nebula