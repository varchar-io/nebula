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

#include "NodeClient.h"

#include "execution/BlockManager.h"

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace service {
namespace node {

using nebula::common::Task;
using nebula::common::TaskState;
using nebula::common::vector_reserve;
using nebula::execution::BlockManager;
using nebula::execution::PhaseType;
using nebula::execution::PlanPtr;
using nebula::execution::TableStates;
using nebula::execution::io::BatchBlock;
using nebula::meta::BlockSignature;
using nebula::meta::BlockState;
using nebula::service::base::BatchSerde;
using nebula::service::base::QuerySerde;
using nebula::service::base::TaskSerde;
using nebula::surface::EmptyRowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::eval::Fields;
using nebula::surface::eval::HistVector;

void NodeClient::echo(const std::string& name) {
  // build request message through fb builder
  flatbuffers::grpc::MessageBuilder mb;
  auto name_offset = mb.CreateString(name);
  auto request_offset = nebula::service::CreateEchoPing(mb, name_offset);
  mb.Finish(request_offset);
  auto request_msg = mb.ReleaseMessage<EchoPing>();

  // a response message placeholder
  flatbuffers::grpc::Message<EchoReply> response_msg;

  grpc::ClientContext context;
  auto status = stub_->Echo(&context, request_msg, &response_msg);
  if (status.ok()) {
    const EchoReply* response = response_msg.GetRoot();
    LOG(INFO) << "From Node Server: " << response->message()->str();
    return;
  }

  LOG(ERROR) << "RPC failed: code=" << status.error_code() << ", msg=" << status.error_message();
}

void NodeClient::echos(const std::string& name, size_t count) {
  flatbuffers::grpc::MessageBuilder mb;
  auto name_offset = mb.CreateString(name);
  auto request_offset = nebula::service::CreateManyEchoPings(mb, name_offset, count);
  mb.Finish(request_offset);
  auto request_msg = mb.ReleaseMessage<ManyEchoPings>();

  flatbuffers::grpc::Message<EchoReply> response_msg;

  grpc::ClientContext context;
  auto stream = stub_->Echos(&context, request_msg);

  // reading back all replies until server says no more (read returns false)
  while (stream->Read(&response_msg)) {
    const EchoReply* response = response_msg.GetRoot();
    LOG(INFO) << "From Node Server: " << response->message()->str();
  }

  auto status = stream->Finish();
  if (!status.ok()) {
    LOG(ERROR) << "RPC failed: code=" << status.error_code() << ", msg=" << status.error_message();
  }
}

folly::Future<RowCursorPtr> NodeClient::execute(const PlanPtr plan) {
  auto p = std::make_shared<folly::Promise<RowCursorPtr>>();
  auto addr = node_.toString();

  // pass values since we reutrn the whole lambda - don't reference temporary things
  // such as local stack allocated variables, including "this" the client itself.
  pool_.add([p, addr, q = query_, plan]() {
    // a response message placeholder
    flatbuffers::grpc::Message<BatchRows> qr;

    const Fields& f = plan->fetch<PhaseType::PARTIAL>().fields();
    auto qp = QuerySerde::serialize(*q, plan->id(), plan->getWindow(), plan->tableVersion());
    grpc::ClientContext context;
    auto channel = ConnectionPool::init()->connection(addr);
    N_ENSURE(channel != nullptr, "requires a valid channel");
    auto stub = nebula::service::NodeServer::NewStub(channel);
    auto status = stub->Query(&context, qp, &qr);
    if (status.ok()) {
      auto& stats = plan->ctx().stats();
      auto fb = BatchSerde::deserialize(&qr, f, stats);
      stats.rowsRet += fb->size();
      VLOG(1) << "Received batch as number of rows: " << fb->size();

      // update into current server block management
      p->setValue(fb);
      return;
    }

    LOG(ERROR) << "Node failure: " << status.error_message() << ". Node: " << addr;
    // else return empty result set
    p->setValue(EmptyRowCursor::instance());
  });

  return p->getFuture();
}

void NodeClient::update() {
  // build request message through fb builder
  flatbuffers::grpc::MessageBuilder mb;
  mb.Finish(nebula::service::CreateNodeStateRequest(mb, 1));
  auto nsRequest = mb.ReleaseMessage<NodeStateRequest>();

  // a response message placeholder
  flatbuffers::grpc::Message<NodeStateReply> nsReply;

  grpc::ClientContext context;
  auto status = stub_->Poll(&context, nsRequest, &nsReply);
  auto bm = BlockManager::init();

  if (status.ok()) {
    const NodeStateReply* response = nsReply.GetRoot();
    auto blocks = response->blocks();
    size_t size = blocks->size();

    // update into current server block management
    TableStates states;
    for (size_t i = 0; i < size; ++i) {
      const DataBlock* db = blocks->Get(i);
      // convert serialized histograms
      auto hists = db->hists();
      auto histSize = hists->size();
      HistVector histograms;
      if (histSize > 0) {
        vector_reserve(histograms, histSize, "NodeClient.udpate");
        std::transform(hists->begin(), hists->end(),
                       std::back_inserter(histograms),
                       [](auto h) { return nebula::surface::eval::from(h->str()); });
      }
      auto block = std::make_shared<BatchBlock>(
        BlockSignature{
          db->table()->str(),
          db->version()->str(),
          db->id(),
          db->time_start(),
          db->time_end(),
          db->spec()->str() },
        node_,
        BlockState{ db->rows(), db->raw_size(), std::move(histograms) });

      // add this block in the new states
      BlockManager::addBlock(states, block);
    }

    // append empty spec from this node
    auto emptySpecs = response->emptySpecs();
    for (auto itr = emptySpecs->begin(); itr != emptySpecs->end(); ++itr) {
      bm->recordEmptySpec(itr->str());
    }

    // TODO(cao): only swap when there is change?
    // swap the new states in
    bm->swap(node_, states);
    return;
  }

  // can not talk to this node, remove it from our state management
  bm->removeNode(node_.toString());

  LOG(ERROR) << "RPC failed to " << node_.server << ": code=" << status.error_code() << ", msg=" << status.error_message();
  // when this happens, we should try to rebuild the channel to this host
  ConnectionPool::init()->reset(node_);
}

TaskState NodeClient::task(const Task& task) {
  grpc::ClientContext context;
  auto message = TaskSerde::serialize(task);
  flatbuffers::grpc::Message<TaskReply> reply;
  auto status = stub_->Task(&context, message, &reply);
  if (status.ok()) {
    auto r = reply.GetRoot();
    return static_cast<TaskState>(r->state());
  }

  return TaskState::UNKNOWN;
}

} // namespace node
} // namespace service
} // namespace nebula