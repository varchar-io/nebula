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
#include <thread>
#include "common/Evidence.h"

#include <folly/init/Init.h>
#include <grpcpp/grpcpp.h>
#include <unordered_map>
#include "NebulaServer.h"
#include "NebulaService.h"
#include "execution/BlockManager.h"
#include "fmt/format.h"
#include "memory/Batch.h"
#include "meta/NBlock.h"
#include "meta/Table.h"
#include "nebula.grpc.pb.h"
#include "storage/CsvReader.h"

/**
 * A cursor template that help iterating a container.
 * (should we use std::iterator instead?)
 */
namespace nebula {
namespace service {

using grpc::ServerContext;
using grpc::Status;
using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::memory::Batch;
using nebula::meta::NBlock;
using nebula::meta::Table;
using nebula::storage::CsvReader;
using nebula::surface::RowCursor;
using nebula::surface::RowData;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::TypeNode;
using nebula::type::TypeSerializer;

grpc::Status V1ServiceImpl::Tables(grpc::ServerContext*, const ListTables* request, TableList* reply) {
  auto bm = BlockManager::init();
  auto limit = request->limit();
  if (limit < 1) {
    limit = 10;
  }

  for (const auto& table : bm->getTables(limit)) {
    reply->add_table(table);
  }

  LOG(INFO) << "Served table list request.";
  return Status::OK;
}

grpc::Status V1ServiceImpl::State(grpc::ServerContext*, const TableStateRequest* request, TableStateResponse* reply) {
  const Table& table = getTable(request->table());
  auto bm = BlockManager::init();
  // query the table's state
  auto metrics = bm->getTableMetrics(table.name());
  reply->set_blockcount(std::get<0>(metrics));
  reply->set_rowcount(std::get<1>(metrics));
  reply->set_memsize(std::get<2>(metrics));
  reply->set_mintime(std::get<3>(metrics));
  reply->set_maxtime(std::get<4>(metrics));

  // TODO(cao) - need meta data system to query table info

  auto schema = table.schema();
  for (size_t i = 0, size = schema->size(); i < size; ++i) {
    auto column = schema->childType(i);
    if (!column->isScalar(column->k())) {
      if (!column->isCompound(column->k())) {
        reply->add_dimension(column->name());
      }
    } else {
      reply->add_metric(column->name());
    }
  }

  LOG(INFO) << "Served table stats request for " << request->table();
  return Status::OK;
}

grpc::Status V1ServiceImpl::Query(grpc::ServerContext*, const QueryRequest* request, QueryResponse* reply) {
  // validate the query request and build the call
  nebula::common::Evidence::Duration tick;
  ErrorCode error = ErrorCode::NONE;
  // compile the query into a plan
  auto plan = handler_.compile(getTable(request->table()), *request, error);
  if (error != ErrorCode::NONE) {
    return replyError(error, reply, 0);
  }

  // display query plan in console
  // plan->display();

  RowCursor result = handler_.query(*plan, error);
  auto durationMs = tick.elapsedMs();
  if (error != ErrorCode::NONE) {
    return replyError(error, reply, durationMs);
  }

  LOG(INFO) << "Finished a query in " << durationMs;

  // return normal serialized data
  auto stats = reply->mutable_stats();
  stats->set_querytimems(durationMs);
  // TODO(cao) - read it from underlying execution
  stats->set_rowsscanned(0);

  // TODO(cao) - use JSON for now, this should come from message request
  // User/client can specify what kind of format of result it expects
  reply->set_type(DataType::JSON);
  reply->set_data(ServiceProperties::jsonify(result, plan->getOutputSchema()));

  return Status::OK;
}

grpc::Status V1ServiceImpl::replyError(ErrorCode code, QueryResponse* reply, size_t durationMs) const {
  N_ENSURE_NE(code, ErrorCode::NONE, "Error Reply Code Not 0");

  auto stats = reply->mutable_stats();
  stats->set_error(code);
  stats->set_message(ServiceProperties::errorMessage(code));
  stats->set_querytimems(durationMs);

  return grpc::Status(grpc::StatusCode::INTERNAL, "error: check stats");
}

void V1ServiceImpl::loadNebulaTest() {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  auto start = Evidence::time("2019-01-01", "%Y-%m-%d");
  auto end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    auto begin = start + i * window;
    bm->add(NBlock(testTable.name(), i++, begin, begin + window));
  }
}

// Logic and data behind the server's behavior.
class EchoServiceImpl final : public Echo::Service {
  Status EchoBack(ServerContext*, const EchoRequest* request, EchoResponse* reply) override {
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
  std::string server_address(fmt::format("0.0.0.0:{0}", nebula::service::ServiceProperties::PORT));
  nebula::service::EchoServiceImpl echoService;
  nebula::service::V1ServiceImpl v1Service;

  // loading data into memory
  LOG(INFO) << "Loading data for table [pin.trends] in single node.";
  v1Service.loadTrends();

  // loading some rand generated data for nebula.test category
  v1Service.loadNebulaTest();

  // loading pins data into memory
  v1Service.loadPins();

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
  folly::init(&argc, &argv);
  FLAGS_logtostderr = 1;

  RunServer();

  return 0;
}