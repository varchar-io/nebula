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

// #ifndef USE_YOMM2_MD
// #define USE_YOMM2_MD
// #endif

#include <gflags/gflags.h>

#include "NodeServer.h"
#include "TaskExecutor.h"
#include "common/Chars.h"
#include "common/Ip.h"
#include "common/TaskScheduler.h"
#include "common/Wrap.h"
#include "execution/BlockManager.h"
#include "execution/core/NodeExecutor.h"
#include "execution/serde/RowCursorSerde.h"
#include "service/client/NebulaClient.h"
#include "surface/DataSurface.h"

DEFINE_int32(MAX_MSG_SIZE, 1073741824, "max message size sending between node and server, default to 1G");
DEFINE_string(NSERVER, "", "discovery server address - host and port");
DEFINE_int32(NODE_PORT, 9199, "port for current node server");

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace service {
namespace node {

using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::common::vector_reserve;
using nebula::execution::BlockManager;
using nebula::execution::PhaseType;
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
#ifdef PPROF
  ProfilerStart("/tmp/ns_query.out");
#endif
  try {
    auto r = query->GetRoot();
    auto q = QuerySerde::deserialize(tableService_, query);
    auto plan = QuerySerde::from(q, r->tstart(), r->tend(), flatbuffers::GetString(r->version()));

    // execute this plan and get results
    NodeExecutor executor(BlockManager::init());
    auto cursor = executor.execute(threadPool_, plan);
    const auto& phase = plan->fetch<PhaseType::PARTIAL>();
    const auto& buffer = nebula::execution::serde::asBuffer(*cursor, phase.outputSchema(), phase.fields());

    // serialize row cursor back
    *batch = BatchSerde::serialize(*buffer, plan);
  } catch (const std::exception& exp) {
    return grpc::Status(grpc::StatusCode::INTERNAL, exp.what());
  }

#ifdef PPROF
  ProfilerStop();
#endif

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
  std::vector<flatbuffers::Offset<DataBlock>> db;
  vector_reserve(db, bm->numBlocks(), "NodeServer.Poll");
  const auto& states = bm->states();
  for (const auto& s : states) {
    s.second->iterate([&mb, &db](const BatchBlock& bb) {
      const auto& state = bb.state();
      // serialize histograms
      std::vector<flatbuffers::Offset<flatbuffers::String>> hists;
      vector_reserve(hists, state.histograms.size(), "NodeServer.Poll.Hists");
      std::transform(state.histograms.begin(), state.histograms.end(),
                     std::back_inserter(hists),
                     [&mb](auto h) {
                       return mb.CreateString(h->toString());
                     });
      db.push_back(CreateDataBlockDirect(
        mb, bb.table().c_str(), bb.version().c_str(), bb.getId(), bb.start(), bb.end(),
        bb.spec().c_str(), bb.storage().c_str(), state.numRows, state.rawSize, &hists));
    });
  }

  // empty specs
  const auto& specSet = bm->emptySpecs();
  std::vector<flatbuffers::Offset<flatbuffers::String>> specs;
  vector_reserve(specs, specSet.size(), "NodeServer.Poll.specs");
  for (const auto& spec : specSet) {
    specs.push_back(mb.CreateString(spec));
  }

  mb.Finish(CreateNodeStateReplyDirect(mb, &db, &specs));

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
  auto task = TaskSerde::deserialize(req);

  // execute it now and return result to client.
  // otherwise enqueue it to async task completion
  TaskState result = TaskState::UNKNOWN;
  if (task.sync()) {
    result = TaskExecutor::singleton().execute(std::move(task));
  } else {
    result = TaskExecutor::singleton().enqueue(std::move(task));
  }

  flatbuffers::grpc::MessageBuilder mb;
  mb.Finish(CreateTaskReply(mb, result));
  *rep = mb.ReleaseMessage<TaskReply>();

  // Return an OK status.
  return grpc::Status::OK;
}

} // namespace node
} // namespace service
} // namespace nebula

std::string ReadNServer() {
  // NCONF is one enviroment variable to overwrite cluster config in the runtime
  if (const char* nConf = std::getenv("NSERVER")) {
    return nConf;
  }

  // reading it from gflags which is usually passed through docker build
  return FLAGS_NSERVER;
}

void RunServer() {
  // global initialization
  nebula::service::base::globalInit();
  nebula::common::Finally f([]() { nebula::service::base::globalExit(); });

  // launch the server
  auto port = FLAGS_NODE_PORT;
  if (port < 1 || port > 65535) {
    LOG(WARNING) << "Invalid port for nebula node, using the default port.";
    port = nebula::service::base::ServiceProperties::NPORT;
  }

  std::string server_address = fmt::format("0.0.0.0:{0}", port);
  nebula::service::node::NodeServerImpl node;

  grpc::ServerBuilder builder;
  builder.SetMaxReceiveMessageSize(FLAGS_MAX_MSG_SIZE);
  builder.SetMaxSendMessageSize(FLAGS_MAX_MSG_SIZE);
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&node);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "Nebula node listening on " << server_address;

  // run a task executor to
  // NOTE that, this is blocking main thread to wait for server down
  // this may prevent system to exit properly, will revisit and revise.
  // run the loop.
  nebula::common::TaskScheduler taskScheduler;
  auto shutdownHandler = [&server, &taskScheduler]() {
    LOG(INFO) << "Shutting down current node...";

    // stop scheduler
    taskScheduler.stop();

    // shut down server
    server->Shutdown();
  };

  taskScheduler.setInterval(
    1000,
    [shutdownHandler, &priorityPool = node.pool()] {
      nebula::service::node::TaskExecutor::singleton().process(shutdownHandler, priorityPool);
    });

  // for every second, ping discovery server
  const auto discovery = ReadNServer();
  const auto client = nebula::service::client::NebulaClient::make(discovery);
  nebula::service::ServiceInfo info;
  if (discovery.size() > 0) {
    // if discovery is localhost, we use localhost to ping as well.
    // otherwise we use dns IPv4 to ping
    constexpr auto LOCAL = "localhost";
    auto hi = nebula::common::Ip::hostInfo(nebula::common::Chars::prefix(
      discovery.data(), discovery.size(), LOCAL, std::strlen(LOCAL)));

    // set the ping info
    info.set_host(hi.host);
    info.set_ipv4(hi.ipv4);
    info.set_port(port);

    taskScheduler.setInterval(
      1000,
      [&client, &info] {
        client.ping(info);
      });
  }

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