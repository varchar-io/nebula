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

#pragma once

#include <glog/logging.h>

#include "NodeClient.h"
#include "NodeConnector.h"
#include "common/Folly.h"
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
  static const std::shared_ptr<NodeConnector> inproc() {
    static const std::shared_ptr<NodeConnector> IN_PROC = std::make_shared<NodeConnector>();
    return IN_PROC;
  }

public:
  ServerExecutor(const std::string& server)
    : server_{ server } {}

  // execute the query plan to get a data set
  nebula::surface::RowCursorPtr execute(folly::ThreadPoolExecutor&,
                                        const PlanPtr,
                                        const std::shared_ptr<NodeConnector> = inproc());

private:
  const std::string server_;
};

} // namespace core
} // namespace execution
} // namespace nebula
