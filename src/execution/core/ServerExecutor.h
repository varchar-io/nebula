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
#include <glog/logging.h>
#include <sys/mman.h>
#include "NodeClient.h"
#include "execution/BlockManager.h"
#include "execution/ExecutionPlan.h"

/**
 * This server executor is the server where the query is accepted from client
 * It does the query compiling and execute the query in the same server.
 */
namespace nebula {
namespace execution {
namespace core {

class ServerExecutor {
public:
  ServerExecutor(const std::string& server)
    : server_{ server }, threadPool_{ 8 } {
    // this servrer should be myself
  }

  NodeClient connect(const nebula::meta::NNode& node);

  nebula::surface::RowCursor execute(ExecutionPlan& plan);

private:
  const std::string server_;
  // TODO(cao) - let's create a global shared pool across all sessions
  folly::CPUThreadPoolExecutor threadPool_;
};

} // namespace core
} // namespace execution
} // namespace nebula
