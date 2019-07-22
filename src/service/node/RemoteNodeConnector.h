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

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include "NodeClient.h"
#include "api/dsl/Query.h"
#include "execution/core/NodeClient.h"
#include "execution/core/NodeConnector.h"
#include "node/node.grpc.fb.h"
#include "node/node_generated.h"
#include "service/nebula/NebulaService.h"

/**
 * Define remote node connector
 */
namespace nebula {
namespace service {
class RemoteNodeConnector : public nebula::execution::core::NodeConnector {
public:
  RemoteNodeConnector(std::shared_ptr<nebula::api::dsl::Query> query) : query_{ query } {}
  virtual ~RemoteNodeConnector() = default;

  virtual std::unique_ptr<nebula::execution::core::NodeClient> makeClient(
    const nebula::meta::NNode& node,
    folly::ThreadPoolExecutor& pool) override {
    //a little optimization here - for in proc node, we can avoid serialization
    if (node.equals(nebula::meta::NNode::inproc())) {
      return std::make_unique<nebula::execution::core::NodeClient>(node, pool);
    }

    return std::make_unique<NodeClient>(node, pool, this->query_);
  }

private:
  std::shared_ptr<nebula::api::dsl::Query> query_;
};
} // namespace service
} // namespace nebula
