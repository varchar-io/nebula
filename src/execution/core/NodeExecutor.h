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

#pragma once

#include <glog/logging.h>
#include <thread>
#include "common/Folly.h"
#include "execution/BlockManager.h"
#include "execution/ExecutionPlan.h"
#include "memory/Batch.h"

/**
 * This node executor accepts one request of an execution plan.
 * And starts to conduct block scan and partial agg operations and return results to the requester.
 */
namespace nebula {
namespace execution {
namespace core {
// This will sit behind the service interface and do the real work.
class NodeExecutor {
public:
  NodeExecutor(const std::shared_ptr<BlockManager> blockManager, bool local = false)
    : blockManager_{ blockManager }, local_{ local } {}

public:
  nebula::surface::RowCursorPtr execute(folly::ThreadPoolExecutor&, const ExecutionPlan&);

private:
  const std::shared_ptr<BlockManager> blockManager_;

  // indicate current node executor is executing the plan with local memory (same process as server)
  const bool local_;
};
} // namespace core
} // namespace execution
} // namespace nebula