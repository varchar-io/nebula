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

#include "ServerExecutor.h"
#include "AggregationMerge.h"
#include "Finalize.h"
#include "NodeConnector.h"
#include "TopSort.h"
#include "common/Folly.h"
#include "surface/eval/UDF.h"

// maximum timeout in ms a query can best do
DEFINE_uint64(RPC_TIMEOUT,
              35000,
              "maximum time nebula can torelate for each query in miliseconds");
DEFINE_bool(SINGLE_NODE, false, "some use case only need to run on single node");

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::meta::NNode;
using nebula::surface::EmptyRowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::SchemaRow;

// set 10 seconds for now as max time to complete a query
static const auto RPC_TIMEOUT = std::chrono::milliseconds(FLAGS_RPC_TIMEOUT);

RowCursorPtr ServerExecutor::execute(
  folly::ThreadPoolExecutor& pool,
  const PlanPtr plan,
  const std::shared_ptr<NodeConnector> connector) {
  std::vector<folly::Future<RowCursorPtr>> results;
  // fetch all nebula nodes that this plan will execute at
  auto nodes = std::vector<nebula::meta::NNode>(plan->getNodes());
  if (FLAGS_SINGLE_NODE && nodes.size() > 1) {
    LOG(ERROR) << "Expected single node but found multiple nodes";
    nodes.erase(nodes.begin(), nodes.end() - 1);
  }

  for (const NNode& node : nodes) {
    auto c = connector->makeClient(node, pool);
    auto f = c->execute(plan)
               // set time out handling
               .onTimeout(RPC_TIMEOUT, [&]() -> RowCursorPtr { 
                 LOG(WARNING) << "RPC Timeout: " << FLAGS_RPC_TIMEOUT;
                 return EmptyRowCursor::instance(); })
               .thenError(folly::tag_t<std::exception>{}, [](const std::exception& e) -> RowCursorPtr {
                 LOG(WARNING) << "RPC Error: " << e.what();
                 return EmptyRowCursor::instance();
               });

    results.push_back(std::move(f));
  }

  // collect all returns and turn it into a future
  auto x = folly::collectAll(results).get();

  // only one result - don't need any aggregation or composite
  const auto& phase = plan->fetch<PhaseType::GLOBAL>();
  const auto& fieldMap = phase.fieldMap();
  if (x.size() == 1) {
    const auto& op = x.at(0);
    if (op.hasException() || !op.hasValue()) {
      return EmptyRowCursor::instance();
    }

    return topSort(finalize(op.value(), fieldMap, phase), phase);
  }

  // multiple results using input schema as output schema used by finalize only
  auto result = merge(pool, phase.inputSchema(), phase.fields(), phase.hasAggregation(), x);

  // result holds the final total rows in the query before applying limit
  auto resultSize = result->size();
  if (resultSize == 0) {
    return result;
  }

  // update result size to stats
  auto& stats = plan->ctx().stats();
  stats.rowsRet = resultSize;

  // apply sorting and limit if available
  return topSort(finalize(result, fieldMap, phase), phase);
}

} // namespace core
} // namespace execution
} // namespace nebula
