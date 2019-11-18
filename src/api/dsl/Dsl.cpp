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
using nebula::meta::NNode;
using nebula::meta::Table;
using nebula::surface::RowData;
using nebula::surface::eval::ValueEval;
using nebula::type::RowType;
using nebula::type::Schema;
using nebula::type::TreeNode;

// TODO(cao) - a better way to handle this is to have struct expression type
// which automatically represents a struct value. Hence col(*) => struct expression.
std::vector<std::shared_ptr<Expression>> Query::preprocess(
  const Schema& schema,
  const std::vector<std::shared_ptr<Expression>>& selects) {
  // 1. expand * if it presents
  const auto size = selects.size();
  std::vector<std::shared_ptr<Expression>> expansion;
  // max capacity
  expansion.reserve(size + schema->size());
  for (size_t i = 0; i < size; ++i) {
    const auto& item = selects.at(i);
    if (item->alias() == Table::ALL_COLUMNS) {
      for (size_t field = 0; field < schema->size(); ++field) {
        // expand in-place
        expansion.push_back(std::make_shared<ColumnExpression>(schema->childType(field)->name()));
      }
    } else {
      expansion.push_back(item);
    }
  }

  // expansion is a copy of selects or a modified version
  return expansion;
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
  // update 11/13/2019:
  // every expression can decide its temporary type and its final type.
  // for example, avg temporary type is int128 or byte[16], but its final type ties to its own type.
  // hence we are extending type method to return mutltiple types for forming schemas for different compute phases

  // fetch schema of the table first
  // validate
  auto schema = table_->schema();

  // build output schema tree
  const auto numOutputFields = this->selects_.size();
  N_ENSURE_GT(numOutputFields, 0, "at least one column select");

  // temp nodes used for block/node computing phase
  // final nodes used for global phase, and they may have different schemas
  std::vector<TreeNode> finalNodes;
  std::vector<TreeNode> tempNodes;
  finalNodes.reserve(numOutputFields);
  tempNodes.reserve(numOutputFields);
  bool differentSchema = false;

  // aggColumns - agg column is marked as true, otherwise false
  std::vector<bool> aggColumns;
  aggColumns.reserve(numOutputFields);
  size_t numAggColumns = 0;

  std::for_each(selects_.begin(), selects_.end(),
                [&finalNodes, &tempNodes, &aggColumns, &numAggColumns, &differentSchema,
                 table = table_](const auto& itr) {
                  // ensure this expression type is populated
                  auto type = itr->type(*table);
                  if (type.native != type.store) {
                    differentSchema = true;
                  }

                  tempNodes.push_back(itr->createStoreType());
                  finalNodes.push_back(itr->createNativeType());

                  auto isAgg = itr->isAgg();
                  aggColumns.push_back(isAgg);
                  if (isAgg) {
                    numAggColumns += 1;
                  }
                });

  // create output schema
  auto tempOutput = std::static_pointer_cast<RowType>(nebula::type::RowType::create("", tempNodes));
  LOG(INFO) << "Query output schema: " << nebula::type::TypeSerializer::to(tempOutput) << " w/ agg columns: " << numAggColumns;

  // if temp schema is different from the final schema
  Schema output = nullptr;
  if (differentSchema) {
    output = std::static_pointer_cast<RowType>(nebula::type::RowType::create("", finalNodes));
    LOG(INFO) << "Query final schema: " << nebula::type::TypeSerializer::to(output);
  }

  // if we have aggregation
  const auto groupSize = groups_.size();
  std::vector<size_t> zbKeys;
  zbKeys.reserve(groupSize);
  if (groupSize > 0) {
    // validations
    // 1. group by index has to be all those columns that are not aggregate columns
    // group by count has to be the same as non-agg column count
    N_ENSURE_EQ(groupSize, numOutputFields - numAggColumns, "query key count");

    // check the index are correct values and convert 1-based group by keys into 0-based keys for internal usage
    std::transform(groups_.begin(), groups_.end(), std::back_inserter(zbKeys), [&aggColumns](size_t index) {
      auto zbIndex = index - 1;
      N_ENSURE(!aggColumns[zbIndex], "group by column should not be aggregate column");
      return zbIndex;
    });
  }

  // check the index are correct values and convert 1-based sort keys into 0-based keys for internal usage
  std::vector<size_t> zbSorts;
  zbSorts.reserve(sorts_.size());
  std::transform(sorts_.begin(), sorts_.end(), std::back_inserter(zbSorts), [&numOutputFields](size_t index) {
    auto zbIndex = index - 1;
    N_ENSURE(index > 0 && index <= numOutputFields, "sort by column is out of range");
    return zbIndex;
  });

  // build block level compute phase
  // TODO(cao) - for some query or aggregate type such as AVG
  // we need to revise the plan to use sum and count to replace.
  // x y, max(z)
  // filter by filter predicate, compute by keys: agg functions
  std::vector<std::unique_ptr<ValueEval>> fields;
  std::transform(selects_.begin(), selects_.end(), std::back_inserter(fields),
                 [](std::shared_ptr<Expression> expr)
                   -> std::unique_ptr<ValueEval> { return expr->asEval(); });
  auto block = std::make_unique<BlockPhase>(schema, tempOutput);
  filter_->type(*table_);
  // a query can have aggregation but no keys, such as "select count(1)"
  (*block)
    .scan(table_->name())
    .filter(filter_->asEval())
    .keys(std::move(zbKeys))
    .compute(std::move(fields))
    .aggregate(numAggColumns, std::move(aggColumns))
    .sort(std::move(zbSorts), sortType_ == SortType::DESC)
    .limit(limit_);

  // partial aggrgation, keys and agg methods
  auto node = std::make_unique<NodePhase>(std::move(block));

  // global aggregation, keys and agg methods
  auto server = std::make_unique<FinalPhase>(std::move(node), output);

  //1. get total nodes that we will run the query, filter_ will help prune results
  auto nodeList = ms_->queryNodes(table_, [](const NNode&) { return true; });

  // 2. gen phase 3 (bottom up) work needs to be done in controller
  LOG(INFO) << "Nodes to execute the query: " << nodeList.size();

  // make an execution plan from a few phases
  return std::make_unique<ExecutionPlan>(
    std::move(server), std::move(nodeList), differentSchema ? output : tempOutput);
}

} // namespace dsl
} // namespace api
} // namespace nebula