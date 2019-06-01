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

#include <folly/init/Init.h>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include "execution/io/trends/Trends.h"
#include "node/node.grpc.fb.h"
#include "node/node_generated.h"
#include "service/nebula/NebulaService.h"

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace service {

class NodeServerImpl final : public NodeServer::Service {
  virtual grpc::Status Echo(
    grpc::ServerContext*,
    const flatbuffers::grpc::Message<EchoPing>*,
    flatbuffers::grpc::Message<EchoReply>*) override;

  virtual grpc::Status Echos(
    grpc::ServerContext* context,
    const flatbuffers::grpc::Message<ManyEchoPings>* request_msg,
    grpc::ServerWriter<flatbuffers::grpc::Message<EchoReply>>* writer)
    override;

  flatbuffers::grpc::MessageBuilder mb_;
  nebula::execution::io::trends::TrendsTable trends_;

public:
  void loadTrends() {
    trends_.load();
  }
};

} // namespace service
} // namespace nebula