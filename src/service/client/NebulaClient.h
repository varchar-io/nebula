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
#include <grpcpp/grpcpp.h>

#include "nebula.grpc.pb.h"

/**
 * A cursor template that help iterating a container.
 * (should we use std::iterator instead?)
 */
namespace nebula {
namespace service {
namespace client {

// A nebula client to connect different tier of servers
class NebulaClient {
public:
  NebulaClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(V1::NewStub(channel)) {}

  // ping nebula server with my data for service discoverys
  std::unique_ptr<PingResponse> ping(const ServiceInfo& si) const noexcept {
    // stub_->
    grpc::ClientContext context;
    auto re = std::make_unique<PingResponse>();
    grpc::Status status = stub_->Ping(&context, si, re.get());
    if (status.ok()) {
      return re;
    } else {
      LOG(INFO) << "Failed to ping: " << status.error_code() << ", " << status.error_message();
      return {};
    }
  }

  static NebulaClient make(const std::string& hostAndPort) {
    return NebulaClient(grpc::CreateChannel(hostAndPort, grpc::InsecureChannelCredentials()));
  }

private:
  std::unique_ptr<V1::Stub> stub_;
};

} // namespace client
} // namespace service
} // namespace nebula