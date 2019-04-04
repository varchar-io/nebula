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
#include "BlockExecutor.h"
#include "meta/MetaService.h"

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::common::CompositeCursor;
using nebula::common::Cursor;
using nebula::memory::Batch;
using nebula::meta::MetaService;
using nebula::surface::MockRowCursor;
using nebula::surface::RowCursor;
using nebula::surface::RowData;
/**
 * Execute a plan on a node level.
 * 
 * TODO(cao) - 
 * This will fanout to multiple blocks in a executor pool before return.
 * So the interfaces will be changed as async interfaces using future and promise.
 */
RowCursor NodeExecutor::execute(const ExecutionPlan& plan) {
  const NodePhase& nodePhase = plan.fetch<PhaseType::PARTIAL>();
  const BlockPhase& blockPhase = plan.fetch<PhaseType::COMPUTE>();
  // query total number of blocks to  executor on and
  // launch block executor on each in parallel
  LOG(INFO) << "start processing...";
  MetaService ms;
  const std::vector<Batch*> blocks = blockManager_->query(*ms.query(blockPhase.table()));

  LOG(INFO) << "Processing total blocks: " << blocks.size();
  std::vector<folly::Future<RowCursor>> results;
  results.reserve(blocks.size());
  std::transform(blocks.begin(), blocks.end(), std::back_inserter(results),
                 [&blockPhase, this](const auto block) {
                   return compute(*block, blockPhase);
                 });

  // compile the results into a single row cursor
  auto x = folly::collectAll(results).get();
  auto c = std::make_shared<CompositeCursor<RowData>>();
  for (auto it = x.begin(); it < x.end(); ++it) {
    c->combine(it->value());
  }

  return c;
}

folly::Future<RowCursor> NodeExecutor::compute(const Batch& block, const BlockPhase& phase) {
  auto p = std::make_shared<folly::Promise<RowCursor>>();
  threadPool_.add([&block, &phase, p]() {
    // compute phase on block and return the result
    p->setValue(std::make_shared<BlockExecutor>(block, phase));
  });

  return p->getFuture();
}

} // namespace core
} // namespace execution
} // namespace nebula
