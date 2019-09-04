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

// #ifndef USE_YOMM2_MD
// #define USE_YOMM2_MD
// #endif

#include "NodeServer.h"
#include "TaskExecutor.h"
#include "common/TaskScheduler.h"
#include "execution/BlockManager.h"
#include "execution/core/NodeExecutor.h"
#include "execution/io/trends/Trends.h"
#include "execution/serde/RowCursorSerde.h"
#include "surface/DataSurface.h"

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace service {
namespace node {

using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::execution::BlockManager;
using nebula::execution::core::NodeExecutor;
using nebula::execution::io::BatchBlock;
using nebula::memory::keyed::FlatBuffer;
using nebula::service::base::BatchSerde;
using nebula::service::base::QuerySerde;
using nebula::service::base::TaskSerde;
using nebula::surface::RowCursorPtr;

// Single echo implementation
grpc::Status NodeServerImpl::Echo(
  grpc::ServerContext*,
  const flatbuffers::grpc::Message<EchoPing>* request_msg,
  flatbuffers::grpc::Message<EchoReply>* response_msg) {
  flatbuffers::grpc::MessageBuilder mb;
  // We call GetRoot to "parse" the message. Verification is already
  // performed by default. See the notes below for more details.
  const EchoPing* request = request_msg->GetRoot();

  // Fields are retrieved as usual with FlatBuffers
  const std::string& name = request->name()->str();

  // `flatbuffers::grpc::MessageBuilder` is a `FlatBufferBuilder` with a
  // special allocator for efficient gRPC buffer transfer, but otherwise
  // usage is the same as usual.
  auto msg_offset = mb.CreateString("Hello, " + name);
  auto hello_offset = CreateEchoReply(mb, msg_offset);
  mb.Finish(hello_offset);

  // The `ReleaseMessage<T>()` function detaches the message from the
  // builder, so we can transfer the resopnse to gRPC while simultaneously
  // detaching that memory buffer from the builer.
  *response_msg = mb.ReleaseMessage<EchoReply>();
  assert(response_msg->Verify());

  // Return an OK status.
  return grpc::Status::OK;
}

// Streaming multiple echos implementation
grpc::Status NodeServerImpl::Echos(
  grpc::ServerContext*,
  const flatbuffers::grpc::Message<ManyEchoPings>* request_msg,
  grpc::ServerWriter<flatbuffers::grpc::Message<EchoReply>>* writer) {
  flatbuffers::grpc::MessageBuilder mb;
  // The streaming usage below is simply a combination of standard gRPC
  // streaming with the FlatBuffers usage shown above.
  const ManyEchoPings* request = request_msg->GetRoot();
  const std::string& name = request->name()->str();
  int num_greetings = request->num_greetings();

  for (int i = 0; i < num_greetings; i++) {
    auto msg_offset = mb.CreateString("Many hellos, " + name);
    auto hello_offset = CreateEchoReply(mb, msg_offset);
    mb.Finish(hello_offset);
    writer->Write(mb.ReleaseMessage<EchoReply>());
  }

  return grpc::Status::OK;
}

// TODO(cao) - we may want to change this endpoint as streaming
// So that we don't need to do aggregation here, instead push all block executor results to server for aggregation.
// Single aggregation - perf?
grpc::Status NodeServerImpl::Query(
  grpc::ServerContext*,
  const flatbuffers::grpc::Message<QueryPlan>* query,
  flatbuffers::grpc::Message<BatchRows>* batch) {
  try {
    auto plan = QuerySerde::from(tableService_, query);

    // execute this plan and get results
    NodeExecutor executor(BlockManager::init());
    auto cursor = executor.execute(*plan);

    const auto& buffer = nebula::execution::serde::asBuffer(*cursor, plan->getOutputSchema());

    // serialize row cursor back
    *batch = BatchSerde::serialize(*buffer);
  } catch (const std::exception& exp) {
    return grpc::Status(grpc::StatusCode::INTERNAL, exp.what());
  }

  return grpc::Status::OK;
}

// poll block status of a node
grpc::Status NodeServerImpl::Poll(
  grpc::ServerContext*,
  const flatbuffers::grpc::Message<NodeStateRequest>* req,
  flatbuffers::grpc::Message<NodeStateReply>* rep) {

  // get node state request
  const NodeStateRequest* request = req->GetRoot();

  // Fields are retrieved as usual with FlatBuffers
  if (request->type() != 1) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "wrong request type");
  }

  // `flatbuffers::grpc::MessageBuilder` is a `FlatBufferBuilder` with a
  // special allocator for efficient gRPC buffer transfer, but otherwise
  // usage is the same as usual.
  const auto bm = BlockManager::init();
  flatbuffers::grpc::MessageBuilder mb;
  auto blocks = bm->all();
  // LOG(INFO) << "Current node hosts # blocks: " << blocks.size();

  std::vector<flatbuffers::Offset<DataBlock>> db;
  db.reserve(blocks.size());
  std::transform(blocks.cbegin(), blocks.cend(),
                 std::back_inserter(db), [&mb](const BatchBlock& bb) {
                   const auto& state = bb.state();
                   return CreateDataBlockDirect(
                     mb, bb.getTable().c_str(), bb.getId(), bb.start(), bb.end(),
                     bb.storage().c_str(), state.numRows, state.rawSize);
                 });

  mb.Finish(CreateNodeStateReplyDirect(mb, &db));

  // The `ReleaseMessage<T>()` function detaches the message from the
  // builder, so we can transfer the resopnse to gRPC while simultaneously
  // detaching that memory buffer from the builer.
  *rep = mb.ReleaseMessage<NodeStateReply>();

  // Return an OK status.
  return grpc::Status::OK;
}

// poll block status of a node
grpc::Status NodeServerImpl::Task(
  grpc::ServerContext*,
  const flatbuffers::grpc::Message<TaskSpec>* req,
  flatbuffers::grpc::Message<TaskReply>* rep) {
  // deserialze a task
  TaskState result = TaskExecutor::singleton().enqueue(
    TaskSerde::deserialize(req));

  flatbuffers::grpc::MessageBuilder mb;
  mb.Finish(CreateTaskReply(mb, result));
  *rep = mb.ReleaseMessage<TaskReply>();

  // Return an OK status.
  return grpc::Status::OK;
}

} // namespace node
} // namespace service
} // namespace nebula

void RunServer() {
  // launch the server
  std::string server_address(fmt::format("0.0.0.0:{0}", nebula::service::base::ServiceProperties::NPORT));
  nebula::service::node::NodeServerImpl node;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&node);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "Nebula node server listening on " << server_address;

  // run a task executor to
  // NOTE that, this is blocking main thread to wait for server down
  // this may prevent system to exit properly, will revisit and revise.
  // run the loop.
  nebula::common::TaskScheduler taskScheduler;
  taskScheduler.setInterval(
    1000,
    [] {
      nebula::service::node::TaskExecutor::singleton().process();
    });

  // NOTE that, this is blocking main thread to wait for server down
  // this may prevent system to exit properly, will revisit and revise.
  // run the loop.
  taskScheduler.run();

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  // executuin serde init
  nebula::execution::serde::init();

  // init folly
  folly::init(&argc, &argv);
  FLAGS_logtostderr = 1;

  RunServer();
  return 0;
}