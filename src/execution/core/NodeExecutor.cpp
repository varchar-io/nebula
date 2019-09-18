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

#include "NodeExecutor.h"

#include <gflags/gflags.h>

#include "AggregationMerge.h"
#include "BlockExecutor.h"
#include "TopSort.h"
#include "execution/eval/UDF.h"
#include "execution/meta/TableService.h"

DEFINE_uint64(TOP_SORT_SCALE,
              0,
              "This defines scale set of top sorting queries, by default, we're returning everything when scale is 0."
              "However, many times we want fast return so that we don't need to serialize massive data back to server,"
              "instead we return scale times of sorting result back to server and this may result in wrong answer!");

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::execution::meta::TableService;
using nebula::memory::Batch;
using nebula::surface::RowCursorPtr;

// distribute the compute task into a promise
folly::Future<RowCursorPtr> dist(
  folly::ThreadPoolExecutor& pool,
  const Batch& block,
  const BlockPhase& phase) {
  auto p = std::make_shared<folly::Promise<RowCursorPtr>>();
  pool.add([&block, &phase, p]() {
    // compute phase on block and return the result
    p->setValue(nebula::execution::core::compute(block, phase));
  });

  return p->getFuture();
}

/**
 * Execute a plan on a node level.
 * 
 * TODO(cao) - 
 * This will fanout to multiple blocks in a executor pool before return.
 * So the interfaces will be changed as async interfaces using future and promise.
 */
RowCursorPtr NodeExecutor::execute(folly::ThreadPoolExecutor& pool, const ExecutionPlan& plan) {
  const BlockPhase& blockPhase = plan.fetch<PhaseType::COMPUTE>();
  // query total number of blocks to  executor on and
  // launch block executor on each in parallel
  // TODO(cao): this table service instance potentially can be carried by a query context on each node
  auto ts = TableService::singleton();
  const std::vector<Batch*> blocks = blockManager_->query(*ts->query(blockPhase.table()), plan);

  LOG(INFO) << "Processing total blocks: " << blocks.size();
  std::vector<folly::Future<RowCursorPtr>> results;
  results.reserve(blocks.size());
  std::transform(blocks.begin(), blocks.end(), std::back_inserter(results),
                 [&blockPhase, &pool](const auto block) {
                   return dist(pool, *block, blockPhase);
                 });

  // compile the results into a single row cursor
  auto x = folly::collectAll(results).get();

  // single response optimization
  if (x.size() == 1) {
    return x.at(0).value();
  }

  // depends on the query plan, if there is no aggregation
  // the results set from different block exeuction can be simply composite together
  // but the query needs to aggregate on keys, then we have to merge the results based on partial aggregatin plan
  const NodePhase& nodePhase = plan.fetch<PhaseType::PARTIAL>();
  auto merged = merge(pool, nodePhase.fields(), nodePhase.hasAgg(), x);

  // if scale is 0 or this query has no limit on it
  if (FLAGS_TOP_SORT_SCALE == 0 || nodePhase.top() == 0) {
    return merged;
  }

  return topSort(merged, plan, FLAGS_TOP_SORT_SCALE);
}

} // namespace core
} // namespace execution
} // namespace nebula
