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

#include "Dsl.h"
#include "common/Cursor.h"
#include "surface/DataSurface.h"

namespace nebula {
namespace api {
namespace dsl {

using nebula::execution::ExecutionPlan;
using nebula::surface::RowData;

// a filter accepts a bool expression as its parameter to evaluate.
Query& Query::where(const Expression<bool>& filter) {
  // apply filter and return myself
  filter_ = std::make_unique<Expression<bool>>(filter);
  return *this;
}

// execute current query to get result list
std::unique_ptr<ExecutionPlan> Query::compile() const {
  // compile the query into an execution plan
  // a valid query (single data source query - no join support at the moment) should be
  // 1. aggregation query, should have more than 1 UDAF in selects
  // 2. sample query, no UDAF involved, only columns, expressions or transformational UDFS involved
  // other basic validations
  // 1. groupby and sortby columns are based on index - they should be valid indices.
  // 2. limit should be always placed?

  // fetch schema of the table first
  // Meta
  // table_
  return nullptr;
}

} // namespace dsl
} // namespace api
} // namespace nebula