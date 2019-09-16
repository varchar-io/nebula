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

#include "ServerExecutor.h"
#include "AggregationMerge.h"
#include "NodeConnector.h"
#include "common/Folly.h"
#include "execution/eval/UDF.h"
#include "memory/keyed/FlatRowCursor.h"
#include "memory/keyed/HashFlat.h"
#include "surface/TopRows.h"

// TODO(cao) - COMMENT OUT, lib link issue
// DEFINE_uint32(NODE_TIMEOUT, 2000, "miliseconds");

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::common::CompositeCursor;
using nebula::common::Cursor;
using nebula::execution::eval::UDAF;
using nebula::memory::keyed::FlatRowCursor;
using nebula::memory::keyed::HashFlat;
using nebula::meta::NNode;
using nebula::surface::EmptyRowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::surface::TopRows;
using nebula::type::Kind;

// set 10 seconds for now as max time to complete a query
static std::chrono::milliseconds RPC_TIMEOUT = std::chrono::milliseconds(10000);

RowCursorPtr ServerExecutor::execute(const ExecutionPlan& plan, const std::shared_ptr<NodeConnector> connector) {
  std::vector<folly::Future<RowCursorPtr>> results;
  for (const NNode& node : plan.getNodes()) {
    auto c = connector->makeClient(node, threadPool_);
    auto f = c->execute(plan)
               // set time out handling
               // TODO(cao) - add error handling too via thenError
               .onTimeout(RPC_TIMEOUT, [&]() -> RowCursorPtr { 
                 LOG(WARNING) << "Timeout: " << RPC_TIMEOUT.count();
                 return EmptyRowCursor::instance(); });

    results.push_back(std::move(f));
  }

  // collect all returns and turn it into a future
  auto x = folly::collectAll(results).get();

  // only one result - don't need any aggregation or composite
  if (x.size() == 1) {
    const auto& op = x.at(0);
    if (op.hasException() || !op.hasValue()) {
      return EmptyRowCursor::instance();
    }

    return topSort(op.value(), plan);
  }

  // multiple results
  const auto& phase = plan.fetch<PhaseType::GLOBAL>();
  auto result = merge(phase.outputSchema(), phase.keys(), phase.fields(), phase.hasAgg(), x);

  // apply sorting and limit if available
  return topSort(result, plan);
}

RowCursorPtr ServerExecutor::topSort(RowCursorPtr input, const ExecutionPlan& plan) {
  // short circuit
  if (input->size() == 0) {
    return input;
  }

  // do the aggregation from all different nodes
  // sort and top of results
  auto schema = plan.getOutputSchema();
  const auto& phase = plan.fetch<PhaseType::GLOBAL>();
  std::function<bool(const std::unique_ptr<RowData>& left, const std::unique_ptr<RowData>& right)> less = nullptr;
  const auto& sorts = phase.sorts();
  if (sorts.size() > 0) {
    N_ENSURE(sorts.size() == 1, "support single sorting column for now");
    const auto& col = schema->childType(sorts[0]);
    const auto kind = col->k();
    const auto& name = col->name();

// instead of assert, we torelate column not found for sorting
#define LESS_KIND_CASE(K, F, OP)                                                                  \
  case Kind::K: {                                                                                 \
    less = [&name](const std::unique_ptr<RowData>& left, const std::unique_ptr<RowData>& right) { \
      return left->F(name) OP right->F(name);                                                     \
    };                                                                                            \
    break;                                                                                        \
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

  return std::make_shared<TopRows>(input, phase.top(), less);
}

} // namespace core
} // namespace execution
} // namespace nebula
