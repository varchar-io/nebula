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
#include <algorithm>
#include "common/Cursor.h"
#include "surface/DataSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace api {
namespace dsl {

using nebula::execution::BlockPhase;
using nebula::execution::ExecutionPlan;
using nebula::execution::FinalPhase;
using nebula::execution::NodePhase;
using nebula::execution::eval::ValueEval;
using nebula::meta::NNode;
using nebula::surface::RowData;
using nebula::type::RowType;
using nebula::type::Schema;
using nebula::type::TreeNode;

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
  // validate
  auto schema = table_->getSchema();

  // build output schema tree
  const auto numOutputFields = this->selects_.size();
  N_ENSURE_GT(numOutputFields, 0, "at least one column select");
  std::vector<TreeNode> children;
  children.reserve(numOutputFields);

  // aggColumns - agg column is marked as true, otherwise false
  std::vector<bool> aggColumns;
  aggColumns.reserve(numOutputFields);
  size_t numAggColumns = 0;

  std::for_each(selects_.begin(), selects_.end(),
                [&children, &aggColumns, &numAggColumns, this](const auto& itr) {
                  children.push_back(itr->type(*table_));
                  auto isAgg = itr->isAgg();
                  aggColumns.push_back(isAgg);
                  if (isAgg) {
                    numAggColumns += 1;
                  }
                });

  // create output schema
  auto output = std::static_pointer_cast<RowType>(nebula::type::RowType::create("", children));
  LOG(INFO) << "Query output schema: " << nebula::type::TypeSerializer::to(output) << " w/ agg columns: " << numAggColumns;

  // validations
  // 1. group by index has to be all those columns that are not aggregate columns
  // group by count has to be the same as non-agg column count
  N_ENSURE_EQ(groups_.size(), numOutputFields - numAggColumns);

  // check the index are correct values
  for (auto& index : groups_) {
    // group/sort by index are all 1-based index
    N_ENSURE(!aggColumns[(index - 1)], "group by column should not be aggregate column");
  }

  // sort columns can be any of the final column
  for (auto& index : sorts_) {
    N_ENSURE(index > 0 && index <= numOutputFields, "sort by column is out of range");
  }

  // build block level compute phase
  // TODO(cao) - for some query or aggregate type such as AVG
  // we need to revise the plan to use sum and count to replace.
  // x y, max(z)
  // filter by filter predicate, compute by keys: agg functions
  std::vector<std::unique_ptr<ValueEval>> fields;
  std::transform(selects_.begin(), selects_.end(), std::back_inserter(fields),
                 [](std::shared_ptr<Expression> expr) -> std::unique_ptr<ValueEval> { return expr->asEval(); });
  auto block = std::make_unique<BlockPhase>(schema, output);
  filter_->type(*table_);
  (*block).scan(table_->name()).filter(filter_->asEval()).keys(groups_).compute(std::move(fields));

  // partial aggrgation, keys and agg methods
  auto node = std::make_unique<NodePhase>(std::move(block));
  (*node).agg();

  // global aggregation, keys and agg methods
  auto controller = std::make_unique<FinalPhase>(std::move(node));
  (*controller).agg().sort(sorts_).limit(limit_);

  //1. get total nodes that we will run the query, filter_ will help prune results
  auto nodeList = ms_->queryNodes(table_, [](const NNode&) { return true; });

  // 2. gen phase 3 (bottom up) work needs to be done in controller
  LOG(INFO) << "found nodes to execute the query: " << nodeList.size();

  // make an execution plan from a few phases
  return std::make_unique<ExecutionPlan>(std::move(controller), std::move(nodeList), output);
}

} // namespace dsl
} // namespace api
} // namespace nebula