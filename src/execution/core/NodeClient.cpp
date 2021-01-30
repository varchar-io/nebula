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

folly::Future<RowCursorPtr> NodeClient::execute(const PlanPtr plan) {
  auto p = std::make_shared<folly::Promise<RowCursorPtr>>();

  // start to full fill the future
  pool_.add([plan, &pool = pool_, p]() {
    NodeExecutor nodeExec(BlockManager::init(), true);
    p->setValue(nodeExec.execute(pool, plan));
  });

  return p->getFuture();
}

} // namespace core
} // namespace execution
} // namespace nebula
