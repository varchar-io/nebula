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

#include "NodeServer.h"

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace service {

// Single echo implementation
grpc::Status NodeServerImpl::Echo(
  grpc::ServerContext* context,
  const flatbuffers::grpc::Message<EchoPing>* request_msg,
  flatbuffers::grpc::Message<EchoReply>* response_msg) {
  // flatbuffers::grpc::MessageBuilder mb_;
  // We call GetRoot to "parse" the message. Verification is already
  // performed by default. See the notes below for more details.
  const EchoPing* request = request_msg->GetRoot();

  // Fields are retrieved as usual with FlatBuffers
  const std::string& name = request->name()->str();

  // `flatbuffers::grpc::MessageBuilder` is a `FlatBufferBuilder` with a
  // special allocator for efficient gRPC buffer transfer, but otherwise
  // usage is the same as usual.
  auto msg_offset = mb_.CreateString("Hello, " + name);
  auto hello_offset = CreateEchoReply(mb_, msg_offset);
  mb_.Finish(hello_offset);

  // The `ReleaseMessage<T>()` function detaches the message from the
  // builder, so we can transfer the resopnse to gRPC while simultaneously
  // detaching that memory buffer from the builer.
  *response_msg = mb_.ReleaseMessage<EchoReply>();
  assert(response_msg->Verify());

  // Return an OK status.
  return grpc::Status::OK;
}

// Streaming multiple echos implementation
grpc::Status NodeServerImpl::Echos(
  grpc::ServerContext* context,
  const flatbuffers::grpc::Message<ManyEchoPings>* request_msg,
  grpc::ServerWriter<flatbuffers::grpc::Message<EchoReply>>* writer) {
  // The streaming usage below is simply a combination of standard gRPC
  // streaming with the FlatBuffers usage shown above.
  const ManyEchoPings* request = request_msg->GetRoot();
  const std::string& name = request->name()->str();
  int num_greetings = request->num_greetings();

  for (int i = 0; i < num_greetings; i++) {
    auto msg_offset = mb_.CreateString("Many hellos, " + name);
    auto hello_offset = CreateEchoReply(mb_, msg_offset);
    mb_.Finish(hello_offset);
    writer->Write(mb_.ReleaseMessage<EchoReply>());
  }

  return grpc::Status::OK;
}

} // namespace service
} // namespace nebula

void RunServer() {
  std::string server_address(fmt::format("0.0.0.0:{0}", nebula::service::ServiceProperties::NPORT));
  nebula::service::NodeServerImpl node;

  // loading data into memory
  LOG(INFO) << "Loading data for table [pin.trends] in single node.";
  node.loadTrends();

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&node);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "Nebula node server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  FLAGS_logtostderr = 1;

  RunServer();
  return 0;
}