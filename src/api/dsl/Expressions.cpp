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
// for column expression
#define LOGICAL_OP_STRING(OP, TYPE)                                                                                                           \
  auto ColumnExpression::operator OP(const std::string& value)->LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<std::string>> { \
    return LogicalExpression<LogicalOp::TYPE, THIS_TYPE, ConstExpression<std::string>>(*this, ConstExpression<std::string>(value));           \
  }

LOGICAL_OP_STRING(==, EQ)
LOGICAL_OP_STRING(>, GT)
LOGICAL_OP_STRING(>=, GE)
LOGICAL_OP_STRING(<, LT)
LOGICAL_OP_STRING(<=, LE)

#undef LOGICAL_OP_STRING

} // namespace dsl
} // namespace api
} // namespace nebula