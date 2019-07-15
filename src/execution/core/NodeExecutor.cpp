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
#include "AggregationMerge.h"
#include "BlockExecutor.h"
#include "execution/eval/UDF.h"
#include "execution/meta/TableService.h"
#include "memory/keyed/FlatRowCursor.h"

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::common::Cursor;
using nebula::execution::eval::UDAF;
using nebula::execution::meta::TableService;
using nebula::memory::Batch;
using nebula::memory::keyed::FlatRowCursor;
using nebula::memory::keyed::HashFlat;
using nebula::surface::CompositeRowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::type::Kind;

/**
 * Execute a plan on a node level.
 * 
 * TODO(cao) - 
 * This will fanout to multiple blocks in a executor pool before return.
 * So the interfaces will be changed as async interfaces using future and promise.
 */
RowCursorPtr NodeExecutor::execute(const ExecutionPlan& plan) {
  const BlockPhase& blockPhase = plan.fetch<PhaseType::COMPUTE>();
  // query total number of blocks to  executor on and
  // launch block executor on each in parallel
  // TODO(cao): this table service instance potentially can be carried by a query context on each node
  TableService ts;
  const std::vector<Batch*> blocks = blockManager_->query(*ts.query(blockPhase.table()), plan);

  LOG(INFO) << "Processing total blocks: " << blocks.size();
  std::vector<folly::Future<RowCursorPtr>> results;
  results.reserve(blocks.size());
  std::transform(blocks.begin(), blocks.end(), std::back_inserter(results),
                 [&blockPhase, this](const auto block) {
                   return compute(*block, blockPhase);
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
  return merge(nodePhase.outputSchema(), nodePhase.keys(), nodePhase.fields(), nodePhase.hasAgg(), x);
}

folly::Future<RowCursorPtr> NodeExecutor::compute(const Batch& block, const BlockPhase& phase) {
  auto p = std::make_shared<folly::Promise<RowCursorPtr>>();
  threadPool_.add([&block, &phase, p]() {
    // compute phase on block and return the result
    p->setValue(nebula::execution::core::compute(block, phase));
  });

  return p->getFuture();
}

} // namespace core
} // namespace execution
} // namespace nebula
