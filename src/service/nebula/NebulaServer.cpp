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

#include <glog/logging.h>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "nebula.grpc.pb.h"

/**
 * A cursor template that help iterating a container.
 * (should we use std::iterator instead?)
 */
namespace nebula {
namespace service {

using grpc::ServerContext;
using grpc::Status;
using nebula::service::Echo;
using nebula::service::EchoRequest;
using nebula::service::EchoResponse;

// Logic and data behind the server's behavior.
class EchoServiceImpl final : public Echo::Service {
  Status EchoBack(ServerContext* context, const EchoRequest* request, EchoResponse* reply) override {
    std::string prefix("This is from nebula: ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }
};

} // namespace service
} // namespace nebula

// NOTE: main function can't be placed inside a namespace
// Otherwise you may get undefined symbol "_main" error in link
void RunServer() {
  std::string server_address("0.0.0.0:9090");
  nebula::service::EchoServiceImpl service;

  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "Server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}