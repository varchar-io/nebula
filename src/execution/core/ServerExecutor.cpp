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
#include "TopSort.h"
#include "common/Folly.h"
#include "execution/eval/UDF.h"

// TODO(cao) - COMMENT OUT, lib link issue
// DEFINE_uint32(NODE_TIMEOUT, 2000, "miliseconds");

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::meta::NNode;
using nebula::surface::EmptyRowCursor;
using nebula::surface::RowCursorPtr;

// set 10 seconds for now as max time to complete a query
static std::chrono::milliseconds RPC_TIMEOUT = std::chrono::milliseconds(10000);

RowCursorPtr ServerExecutor::execute(
  folly::ThreadPoolExecutor& pool,
  const ExecutionPlan& plan,
  const std::shared_ptr<NodeConnector> connector) {
  std::vector<folly::Future<RowCursorPtr>> results;
  for (const NNode& node : plan.getNodes()) {
    auto c = connector->makeClient(node, pool);
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
  auto result = merge(pool, phase.fields(), phase.hasAgg(), x);

  // apply sorting and limit if available
  return topSort(result, plan);
}

} // namespace core
} // namespace execution
} // namespace nebula
