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
#include <unordered_map>
#include "NebulaServer.h"
#include "execution/BlockManager.h"
#include "memory/Batch.h"
#include "meta/NBlock.h"
#include "meta/Table.h"
#include "nebula.grpc.pb.h"
#include "storage/CsvReader.h"
#include "type/Serde.h"

/**
 * A cursor template that help iterating a container.
 * (should we use std::iterator instead?)
 */
namespace nebula {
namespace service {

using grpc::ServerContext;
using grpc::Status;
using nebula::execution::BlockManager;
using nebula::memory::Batch;
using nebula::meta::NBlock;
using nebula::meta::Table;
using nebula::service::Echo;
using nebula::service::EchoRequest;
using nebula::service::EchoResponse;
using nebula::service::TableStateRequest;
using nebula::service::TableStateResponse;
using nebula::service::V1;
using nebula::storage::CsvReader;
using nebula::type::TypeSerializer;

grpc::Status V1ServiceImpl::State(grpc::ServerContext* context, const TableStateRequest* request, TableStateResponse* reply) {
  auto bm = BlockManager::init();
  // query the table's state
  auto metrics = bm->getTableMetrics(request->table());
  reply->set_blockcount(std::get<0>(metrics));
  reply->set_rowcount(std::get<1>(metrics));
  reply->set_memsize(std::get<2>(metrics));

  return Status::OK;
}

void V1ServiceImpl::loadTrends() {
  // load test data to run this query
  auto bm = BlockManager::init();

  // PURE TEST CODE: load data from a file path
  auto file = "/tmp/pin.trends.csv";

  // load the data into batch based on block.id * 50000 as offset so that we can keep every 50K rows per block
  CsvReader reader(file);
  // every 100K rows, we split it into a block
  const auto bRows = 100000;
  std::unique_ptr<Batch> block = nullptr;
  size_t cursor = 0;
  size_t blockId = 0;
  while (reader.hasNext()) {
    // create a new block
    if (cursor % bRows == 0) {
      // add current block if it's the first time init
      if (cursor > 0) {
        N_ENSURE(block != nullptr, "block should be valid");
        NBlock b(table_, blockId);
        LOG(INFO) << " adding one block: " << (block != nullptr) << ", rows=" << cursor;
        bm->add(b, std::move(block));
      }

      // create new block data
      blockId += 1;
      block = std::make_unique<Batch>(table_.getSchema());
    }

    cursor++;
    auto& row = reader.next();
    // add all entries belonging to test_data_limit_100000 into the block
    block->add(row);
  }

  // add last block
  NBlock b(table_, blockId);
  LOG(INFO) << " adding last block: " << (block != nullptr) << ", rows=" << cursor;
  bm->add(b, std::move(block));
}

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
  nebula::service::EchoServiceImpl echoService;
  nebula::service::V1ServiceImpl v1Service;

  // loading data into memory
  LOG(INFO) << "Loading data for table [pin.trends] in single node.";
  v1Service.loadTrends();

  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&echoService).RegisterService(&v1Service);
  // Finally assemble the server.
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "Nebula server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}