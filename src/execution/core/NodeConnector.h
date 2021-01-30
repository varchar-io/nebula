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
#include "common/Folly.h"
#include "meta/NNode.h"

/**
 * Node connector provides way to connect a node.
 * It could be a in-process provider or network-endpoint provider
 */
namespace nebula {
namespace execution {
namespace core {
class NodeConnector {
public:
  NodeConnector() = default;
  virtual ~NodeConnector() = default;
  virtual std::unique_ptr<NodeClient> makeClient(const nebula::meta::NNode& node, folly::ThreadPoolExecutor& pool) {
    return std::make_unique<NodeClient>(node, pool);
  }
};
} // namespace core
} // namespace execution
} // namespace nebula