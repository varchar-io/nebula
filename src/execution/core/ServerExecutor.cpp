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

#include "ServerExecutor.h"
#include <folly/executors/ThreadedExecutor.h>
#include <folly/futures/Future.h>

// TODO(cao) - COMMENT OUT, lib link issue
// DEFINE_uint32(NODE_TIMEOUT, 2000, "miliseconds");

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::common::CompositeCursor;
using nebula::common::Cursor;
using nebula::meta::NNode;
using nebula::surface::RowCursor;
using nebula::surface::RowData;

static std::chrono::milliseconds RPC_TIMEOUT = std::chrono::milliseconds(2000);

RowCursor ServerExecutor::execute(const ExecutionPlan& plan) {
  std::vector<folly::Future<RowCursor>> results;
  for (const NNode& node : plan.getNodes()) {
    auto c = connect(node);
    auto f = c.execute(plan)
               // set time out handling
               // TODO(cao) - add error handling too via thenError
               .onTimeout(RPC_TIMEOUT, [&]() -> RowCursor { 
                 LOG(WARNING) << "Timeout: " << RPC_TIMEOUT.count();
                 return {}; });

    results.push_back(std::move(f));
  }

  // collect all returns and turn it into a future
  auto x = folly::collectAll(results).get();
  auto c = std::make_shared<CompositeCursor<RowData>>();
  for (auto it = x.begin(); it < x.end(); ++it) {
    if (it->hasValue()) {
      auto cursor = it->value();
      if (cursor) {
        c->combine(cursor);
        continue;
      }
    }

    LOG(INFO) << "A node doesn't return value: error or timeout";
  }

  // do the aggregation from all different nodes
  return c;
}

NodeClient ServerExecutor::connect(const nebula::meta::NNode& node) {
  // connect to the specified node and return an inttance of it.
  return NodeClient(node, threadPool_);
}

} // namespace core
} // namespace execution
} // namespace nebula
