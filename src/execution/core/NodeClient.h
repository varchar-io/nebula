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
#include "common/Folly.h"
#include "execution/BlockManager.h"
#include "execution/ExecutionPlan.h"

/**
 * This node executor accepts one request of an execution plan.
 * And starts to conduct block scan and partial agg operations and return results to the requester.
 */
namespace nebula {
namespace execution {
namespace core {

class NodeClient {
public:
  NodeClient(const nebula::meta::NNode& node, folly::ThreadPoolExecutor& pool)
    : node_{ node }, pool_{ pool } {}
  virtual ~NodeClient() = default;

  virtual folly::Future<nebula::surface::RowCursorPtr> execute(const ExecutionPlan& plan);

  // state is used to pull state of a node - do nothing for inproc node client
  virtual void state(){};

protected:
  static nebula::surface::RowCursorPtr invokeNode(const ExecutionPlan& plan);

protected:
  nebula::meta::NNode node_;
  folly::ThreadPoolExecutor& pool_;
};
} // namespace core
} // namespace execution
} // namespace nebula