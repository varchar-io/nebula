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

#include "NodeClient.h"
#include "NodeExecutor.h"

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::common::Cursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;

RowCursorPtr NodeClient::invokeNode(const ExecutionPlan& plan) {
  // TODO(cao): replace with RPC call to the real node, use the in-node call for now
  NodeExecutor nodeExec(BlockManager::init());
  LOG(INFO) << "Running a node executor: " << plan.id();
  return nodeExec.execute(plan);
}

folly::Future<RowCursorPtr> NodeClient::execute(const ExecutionPlan& plan) {
  auto p = std::make_shared<folly::Promise<RowCursorPtr>>();

  // start to full fill the future
  // TODO(cao) - I don't know yet why using [&] capture for p here doesn't work.
  // life time issue? changing it to a class member doesn't help.
  pool_.add([&plan, p]() {
    p->setValue(invokeNode(plan));
  });

  return p->getFuture();
}

} // namespace core
} // namespace execution
} // namespace nebula
