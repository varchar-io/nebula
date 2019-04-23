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

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using nebula::service::Echo;
using nebula::service::EchoRequest;
using nebula::service::EchoResponse;

class EchoClient {
public:
  EchoClient(std::shared_ptr<Channel> channel)
    : stub_(Echo::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string echo(const std::string& user) {
    // Data we are sending to the server.
    EchoRequest request;
    request.set_name(user);

    // Container for the data we expect from the server.
    EchoResponse reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->EchoBack(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

private:
  std::unique_ptr<Echo::Stub> stub_;
};

} // namespace service
} // namespace nebula

int main(int argc, char** argv) {
  nebula::service::EchoClient greeter(grpc::CreateChannel(
    "localhost:9090", grpc::InsecureChannelCredentials()));
  LOG(INFO) << "Greeter received: " << greeter.echo("nebula");
  return 0;
}