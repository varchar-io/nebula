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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <thread>
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
  NodeExecutor(const std::shared_ptr<BlockManager> blockManager)
    : blockManager_{ blockManager }, threadPool_{ std::thread::hardware_concurrency() } {}

public:
  nebula::surface::RowCursorPtr execute(const ExecutionPlan& plan);

private:
  folly::Future<nebula::surface::RowCursorPtr> compute(const nebula::memory::Batch&, const BlockPhase&);

private:
  const std::shared_ptr<BlockManager> blockManager_;
  folly::CPUThreadPoolExecutor threadPool_;
};
} // namespace core
} // namespace execution
} // namespace nebula