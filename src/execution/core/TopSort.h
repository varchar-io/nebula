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

#include "execution/ExecutionPlan.h"
#include "surface/DataSurface.h"
#include "surface/TopRows.h"

/**
 * A logic wrapper to return top sort cursors when sorting and limiting are present
 */
namespace nebula {
namespace execution {
namespace core {

// SCALE is used to enlarge the final set in result, by default return whatever asked
template <nebula::execution::PhaseType PT>
nebula::surface::RowCursorPtr topSort(
  nebula::surface::RowCursorPtr input,
  const nebula::execution::Phase<PT>& phase,
  size_t scale = 1) {
  // short circuit
  if (input->size() == 0) {
    return input;
  }

  // do the aggregation from all different nodes
  // sort and top of results
  auto schema = phase.outputSchema();
  std::function<bool(const std::unique_ptr<nebula::surface::RowData>& left,
                     const std::unique_ptr<nebula::surface::RowData>& right)>
    less = nullptr;
  const auto& sorts = phase.sorts();
  LOG(INFO) << "Sort the single query result and return: " << sorts.size();
  if (sorts.size() > 0) {
    N_ENSURE(sorts.size() == 1, "support single sorting column for now");
    const auto& col = schema->childType(sorts[0]);
    const auto kind = col->k();
    const auto& name = col->name();

// instead of assert, we torelate column not found for sorting
#define LESS_KIND_CASE(K, F, OP)                                             \
  case nebula::type::Kind::K: {                                              \
    less = [&name](const std::unique_ptr<nebula::surface::RowData>& left,    \
                   const std::unique_ptr<nebula::surface::RowData>& right) { \
      return left->F(name) OP right->F(name);                                \
    };                                                                       \
    break;                                                                   \
  }
#define LESS_SWITCH_DESC(OP)                \
  switch (kind) {                           \
    LESS_KIND_CASE(BOOLEAN, readBool, OP)   \
    LESS_KIND_CASE(TINYINT, readByte, OP)   \
    LESS_KIND_CASE(SMALLINT, readShort, OP) \
    LESS_KIND_CASE(INTEGER, readInt, OP)    \
    LESS_KIND_CASE(BIGINT, readLong, OP)    \
    LESS_KIND_CASE(REAL, readFloat, OP)     \
    LESS_KIND_CASE(DOUBLE, readDouble, OP)  \
    LESS_KIND_CASE(VARCHAR, readString, OP) \
  default:                                  \
    less = nullptr;                         \
  }

    if (phase.isDesc()) {
      LESS_SWITCH_DESC(<)
    } else {
      LESS_SWITCH_DESC(>)
    }

#undef LESS_SWITCH_DESC
#undef LESS_KIND_CASE
  }

  return std::make_shared<nebula::surface::TopRows>(input, phase.top() * scale, std::move(less));
}

} // namespace core
} // namespace execution
} // namespace nebula