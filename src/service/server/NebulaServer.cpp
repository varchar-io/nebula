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

#include "NebulaServer.h"

#include <cstdlib>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <rapidjson/document.h>
#include <signal.h>
#include <string>
#include <thread>

#include "NodeSync.h"
#include "common/Chars.h"
#include "common/Evidence.h"
#include "common/Folly.h"
#include "common/Format.h"
#include "common/Params.h"
#include "common/Spark.h"
#include "common/TaskScheduler.h"
#include "execution/BlockManager.h"
#include "execution/meta/TableService.h"
#include "memory/Batch.h"
#include "meta/ClusterInfo.h"
#include "meta/NBlock.h"
#include "meta/NNode.h"
#include "meta/TableSpec.h"
#include "nebula.grpc.pb.h"
#include "service/base/NebulaService.h"
#include "service/node/ConnectionPool.h"
#include "service/node/RemoteNodeConnector.h"
#include "storage/NFS.h"
#include "storage/NFileSystem.h"

// use "host.docker.internal" for docker env
DEFINE_string(CLS_CONF, "configs/cluster.yml", "cluster config file");
DEFINE_uint64(CLS_CONF_UPDATE_INTERVAL, 3000, "interval in milliseconds to update cluster config");
DEFINE_uint64(NODE_SYNC_INTERVAL, 500, "interval in ms to conduct node sync");
DEFINE_uint32(MAX_TABLES_RETURN, 500, "max tables to fetch to display");
DEFINE_int32(MAX_MSG_SIZE, 67108864, "max message size sending between server and client, default to 64M");

/**
 * A cursor template that help iterating a container.
 * (should we use std::iterator instead?)
 */
namespace nebula {
namespace service {
namespace server {

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using nebula::common::Chars;
using nebula::common::Evidence;
using nebula::common::Identifiable;
using nebula::common::SingleCommandTask;
using nebula::common::Task;
using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::execution::BlockManager;
using nebula::execution::QueryContext;
using nebula::execution::io::BlockLoader;
using nebula::execution::meta::TableService;
using nebula::ingest::IngestSpec;
using nebula::memory::Batch;
using nebula::meta::ClusterInfo;
using nebula::meta::NNode;
using nebula::meta::NRole;
using nebula::meta::SpecState;
using nebula::meta::Table;
using nebula::meta::TableSpec;
using nebula::meta::TableSpecPtr;
using nebula::service::ServiceInfo;
using nebula::service::ServiceTier;
using nebula::service::base::ErrorCode;
using nebula::service::base::ServiceProperties;
using nebula::service::node::RemoteNodeConnector;
using nebula::storage::NFileSystem;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::TypeNode;
using nebula::type::TypeSerializer;

Status V1ServiceImpl::Tables(ServerContext*, const ListTables* request, TableList* reply) {
  auto bm = BlockManager::init();
  auto limit = request->limit();
  if (limit < 1) {
    limit = FLAGS_MAX_TABLES_RETURN;
  }

  for (const auto& table : bm->tables(limit)) {
    // non-existing table has data in system.
    // kinda of leak, log a warning.
    if (!TableService::singleton()->exists(table)) {
      LOG(WARNING) << "Found data for non-existing table.";
      continue;
    }

    reply->add_table(table);
  }

  LOG(INFO) << "Served table list request.";
  return Status::OK;
}

Status V1ServiceImpl::State(ServerContext*, const TableStateRequest* request, TableStateResponse* reply) {
  const auto& tbl = request->table();
  LOG(INFO) << "Look up state for table " << tbl;
  if (tbl.size() == 0) {
    return Status::CANCELLED;
  }

  // the table is probably still in process if empty (schema not set yet)
  auto table = TableService::singleton()->query(tbl).table();
  if (table == nullptr) {
    return Status(StatusCode::NOT_FOUND, "table not found");
  }

  // no schema found for the table
  if (table->empty()) {
    return Status(StatusCode::UNKNOWN, "probably in process");
  }

  auto bm = BlockManager::init();
  // query the table's state
  auto stats = bm->metrics(table->name());
  auto& metrics = stats.metrics;
  reply->set_blockcount(metrics.numBlocks());
  reply->set_rowcount(metrics.numRows());
  reply->set_memsize(metrics.rawBytes());
  auto window = metrics.timeWindow();
  reply->set_mintime(window.first);
  reply->set_maxtime(window.second);

  auto& h = metrics.hists();

  // TODO(cao) - need meta data system to query table info
  auto schema = table->schema();
  for (size_t i = 0, size = schema->size(); i < size; ++i) {
    auto column = schema->childType(i);
    if (!column->isScalar(column->k())) {
      if (!column->isCompound(column->k())) {
        reply->add_dimension(column->name());
      }
    } else {
      reply->add_metric(column->name());
    }

    // send all colummn histograms but it maybe empty
    // if the table has no data blocks (expired)
    if (i < h.size()) {
      reply->add_hists(h.at(i)->toString());
    }
  }

  LOG(INFO) << "Served table stats request for " << request->table();
  return Status::OK;
}

Status V1ServiceImpl::Nuclear(ServerContext* ctx, const EchoRequest* req, EchoResponse* reply) {
  constexpr auto NUCLEAR = "_nuclear_";
  // message verification to ensure
  if (NUCLEAR == req->name()) {
    LOG(INFO) << "Received a nuclear command, tearing down everything";
    // DEBUG/PROFILE PURPOSE:
    // shutdown the local node by this command
    RemoteNodeConnector connector{ nullptr };
    auto nodes = ClusterInfo::singleton().nodes();
    N_ENSURE(nodes.size() > 0, "cluster info has no nodes??");
    auto client = connector.makeClient(*nodes.begin(), threadPool_);
    Task task(TaskType::COMMAND, SingleCommandTask::shutdown());
    client->task(task);

    // shutdown nebula server as well
    if (this->shutdownHandler_) {
      this->shutdownHandler_();
    }

    return Status::OK;
  }

  LOG(WARNING) << "Required message to be correct to shutdown single working node";
  return Status::CANCELLED;
}

std::unique_ptr<QueryContext> buildQueryContext(ServerContext* ctx) {
  // build query context
  const auto& metadata = ctx->client_metadata();

  // fetch user info keys, refer userInfo definition in node.js serving http traffic
  // auth, authorization, user, groups
  std::string user{ "unauth" };
  nebula::common::unordered_set<std::string> groups;
  auto itr = metadata.find("nebula-auth");
  if (itr != metadata.end() && itr->second == "1") {
    auto u = metadata.find("nebula-user");
    if (u != metadata.end()) {
      user = std::string(u->second.data(), u->second.size());
    }

    auto g = metadata.find("nebula-groups");
    if (g != metadata.end()) {
      groups = Chars::split(g->second.data(), g->second.size());
    }
  }

  return std::make_unique<QueryContext>(user, std::move(groups));
}

Status V1ServiceImpl::Load(ServerContext* ctx, const LoadRequest* req, LoadResponse* reply) {
  Evidence::Duration tick;
  // get query context
  auto context = buildQueryContext(ctx);
  LOG(INFO) << "Load data source for user: " << context->user();

  LoadError err = LoadError::SUCCESS;
  TableSpecPtr tbSpec;

  // support unload a table from the cluster on demand
  auto isUnload = false;

  // load request as specs belonging to a unique table name
  switch (req->type()) {
  case LoadType::CONFIG:
    tbSpec = loadHandler_.loadConfigured(req, err);
    break;
  case LoadType::GOOGLE_SHEET:
    tbSpec = loadHandler_.loadGoogleSheet(req, err);
    break;
  case LoadType::DEMAND:
    tbSpec = loadHandler_.loadDemand(req, err);
    break;
  case LoadType::UNLOAD:
    isUnload = true;
    break;
  default:
    err = LoadError::NOT_SUPPORTED;
    break;
  }

  // if load failed, will not consume the load result at all
  if (err != LoadError::SUCCESS) {
    reply->set_error(err);
    return Status::CANCELLED;
  }

  // requested table name or overwritten name
  auto tableName = req->table();

  // for unload case, let's just unload the table from the system
  if (isUnload) {
    TableService::singleton()->unload(tableName);
    reply->set_error(LoadError::SUCCESS);
    return Status::OK;
  }

  // add this table spec to cluster info - auto dedup
  // tbSpec could be null if request is a permanent table
  if (tbSpec) {
    tableName = tbSpec->name;
    ClusterInfo::singleton().addTable(tbSpec);

    // check if we have data for this table already
    // if we have no data blocks - assuming it's loading
    // but in fact, it could be failed too
    const auto stats = BlockManager::init()->metrics(tableName);
    const auto& metrics = stats.metrics;
    if (metrics.numBlocks() == 0) {
      // no blocks found in any node, assuming it's loading
      auto numNodes = stats.nodes.size();
      if (numNodes == 0) {
        err = LoadError::IN_LOADING;
      } else {
        // this seems like a problem - we have nodes but no data blocks
        err = LoadError::EMPTY_RESULT;
        LOG(WARNING) << "Table " << tableName << " has no data blocks in nodes: " << numNodes;
      }
    }
  }

  // always indicating loading using async communication
  reply->set_error(err);
  reply->set_loadtimems(tick.elapsedMs());
  reply->set_table(tableName);
  return Status::OK;
}

Status V1ServiceImpl::Query(ServerContext* ctx, const QueryRequest* request, QueryResponse* reply) {
  // validate the query request and build the call
  Evidence::Duration tick;
  ErrorCode error = ErrorCode::NONE;

  const auto tableName = request->table();
  // get the table registry and activate it by recording latest used time
  // so whenever a data set is queried, its expire time will be extended.
  TableService::singleton()->hit(tableName);

  // build the query
  const auto& tr = TableService::singleton()->query(tableName);
  auto query = handler_.build(*tr.table(), *request, error);
  if (error != ErrorCode::NONE) {
    return replyError(error, reply, 0);
  }

  // get query context
  auto context = buildQueryContext(ctx);
  const auto user = context->user();

  // compile query into a query plan
  auto plan = handler_.compile(
    query, tr.version(), { request->start(), request->end() }, std::move(context), error);
  if (error != ErrorCode::NONE) {
    return replyError(error, reply, 0);
  }

  LOG(INFO) << "Compiled a query to table: " << tableName;
  // create a remote connector and execute the query plan
  auto connector = std::make_shared<RemoteNodeConnector>(query);
  RowCursorPtr result = handler_.query(threadPool_, plan, connector, error);
  const auto durationMs = tick.elapsedMs();
  if (error != ErrorCode::NONE) {
    return replyError(error, reply, durationMs);
  }

  LOG(INFO) << "Finished a query to table: " << tableName;

  // return normal serialized data
  auto& queryStats = plan->ctx().stats();
  auto stats = reply->mutable_stats();
  stats->set_querytimems(durationMs);
  stats->set_rowsscanned(queryStats.rowsScan);
  stats->set_blocksscanned(queryStats.blocksScan);
  stats->set_rowsreturn(queryStats.rowsRet);
  tick.reset();

  // User/client can specify what kind of format of result it expects
  reply->set_type(DataType::JSON);
  auto payload = ServiceProperties::jsonify(result, plan->getOutputSchema());
  reply->set_data(std::move(payload));

  // ttime: transfer time = result serialization time
  LOG(INFO) << "[Query] user=" << user << ", table=" << tableName << ", latency=" << durationMs;
  return Status::OK;
}

Status V1ServiceImpl::replyError(ErrorCode code, QueryResponse* reply, size_t durationMs) const {
  N_ENSURE_NE(code, ErrorCode::NONE, "Error Reply Code Not 0");

  auto error = ServiceProperties::errorMessage(code);
  auto stats = reply->mutable_stats();
  stats->set_error(code);
  stats->set_message(error);
  stats->set_querytimems(durationMs);

  return Status(StatusCode::OK, error);
}

grpc::Status V1ServiceImpl::Url(grpc::ServerContext*, const UrlData* req, UrlData* res) {
  const auto& code = req->code();
  const auto& raw = req->raw();
  // if code is available, fill the raw url into raw field
  if (code.size() > 0) {
    auto url = handler_.meta()->getUrl(code);
    res->set_code(code);
    res->set_raw(url);
  } else if (raw.size() > 0) {
    auto token = handler_.meta()->shortenUrl(raw);
    res->set_code(token);
    res->set_raw(raw);
  }

  // no matter what, always return ok, client side will handle if result is not ideal
  return Status::OK;
}

NRole fromTier(ServiceTier tier) {
  switch (tier) {
  case ServiceTier::NODE: return NRole::NODE;
  default:
    return NRole::SERVER;
  }
}

grpc::Status V1ServiceImpl::Ping(grpc::ServerContext*, const ServiceInfo* si, PingResponse*) {
  NNode node{ fromTier(si->tier()), si->ipv4(), si->port() };
  if (node.server.size() == 0 || node.port == 0) {
    return grpc::Status(StatusCode::INVALID_ARGUMENT,
                        fmt::format("Invalid node info: {0}", node.toString()));
  }

  // refresh this node status in node manager
  ClusterInfo::singleton().nodeManager().update(node);

  return Status::OK;
}

} // namespace server
} // namespace service
} // namespace nebula

std::string LoadClusterConfig() {
  // NCONF is one enviroment variable to overwrite cluster config in the runtime
  if (const char* nConf = std::getenv("NCONF")) {
    return nConf;
  }

  // reading it from gflags which is usually passed through docker build
  return FLAGS_CLS_CONF;
}

void signalHandler(int signum) {
  static std::atomic<bool> signaled(false);

  LOG(INFO) << "Exiting nebula server by signal: " << signum;
  // if bestEffortDie failed again with another signal, this will cause an infinite loop
  // and definitely it's not good to catch it as normal.
  // https://code.google.com/archive/p/segvcatch/ is alternative way to make it as exception.
  if (signaled == false) {
    signaled = true;
    nebula::service::base::bestEffortDie();
  }

  exit(signum);
}

// NOTE: main function can't be placed inside a namespace
// Otherwise you may get undefined symbol "_main" error in link
void RunServer() {
  // register signal handler for all signals
  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);
  signal(SIGABRT, signalHandler);

  // global initialization
  nebula::service::base::globalInit();
  nebula::common::Finally f([]() { nebula::service::base::globalExit(); });

  std::string server_address(fmt::format("0.0.0.0:{0}", nebula::service::base::ServiceProperties::PORT));
  nebula::service::server::V1ServiceImpl v1Service;

  grpc::ServerBuilder builder;
  // set max message send/receive
  builder.SetMaxReceiveMessageSize(FLAGS_MAX_MSG_SIZE);
  builder.SetMaxSendMessageSize(FLAGS_MAX_MSG_SIZE);
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&v1Service);
  // set compression level as medium
  builder.SetDefaultCompressionLevel(GRPC_COMPRESS_LEVEL_MED);
  // Finally assemble the server.
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "Nebula server listening on " << server_address;

  // TODO (cao): start a thread to sync up with etcd setup for cluster info.
  // register cluster info, we're using two different time based scheduelr currently
  // one is NodeSync uses folly::FunctionScheduler and TaskScheduler is built on top of EventBase
  // We're using task scheduler to pull all config changes and spec generation, assignment
  // NodeSync will be responsible to pull all state info from each Node and update them in server
  nebula::common::TaskScheduler taskScheduler;
  v1Service.setShutdownHandler([&server, &taskScheduler]() {
    // tear down everything in one second
    taskScheduler.setTimeout(1000, [&server, &taskScheduler]() {
      LOG(INFO) << "Shutting down nebula server...";

      // stop scheduler
      taskScheduler.stop();

      // shut down server
      server->Shutdown();
    });
  });

  // a counter indicating total number of updates executed
  auto updates = 0;
  std::string confSignature;
  taskScheduler.setInterval(
    FLAGS_CLS_CONF_UPDATE_INTERVAL,
    [&confSignature, &updates] {
      // load config into cluster info
      auto conf = LoadClusterConfig();

      // if the conf is a S3 file, download it and replace the conf as the local copy
      auto uri = nebula::storage::parse(conf);
      bool copied = false;
      auto localFs = nebula::storage::makeFS("local");
      if (!uri.schema.empty()) {
        // create a non-local file system
        auto fs = nebula::storage::makeFS(uri.schema, uri.host);
        conf = localFs->temp();
        N_ENSURE(fs->copy(uri.path, conf), "failed to copy config file");
        copied = true;
      }

      // assuming everything else are local file - if not, let runtime fails you
      auto fi = localFs->info(conf);
      auto sign = fi.signature();

      // for copied data, they will have random name and latest timestamp
      // so we use size and content hash as its signature
      if (copied) {
        const auto size = fi.size;
        auto data = std::make_unique<char[]>(size);
        N_ENSURE_EQ(localFs->read(conf, data.get(), size), size, "should read all bytes of the conf file");
        sign = fmt::format("{0}_{1}", size, nebula::common::Hasher::hash64(data.get(), size));
      }

      auto& ci = nebula::meta::ClusterInfo::singleton();
      if (sign != confSignature || ci.shouldReload()) {
        LOG(INFO) << "Loading nebula cluster config: " << conf << ", old sign: " << confSignature
                  << ", new sign: " << sign << ", size: " << fi.size;

        // update the sign
        confSignature = sign;

        // load the new conf
        ci.load(conf, nebula::service::base::createMetaDB);
      }

      // if the file is copied tmp file, delete it
      if (copied) {
        unlink(conf.c_str());
      }

      { // backup metadb
        if (ci.db().backup()) {
          LOG(INFO) << "complete backup meta db.";
        }
      }

      { // re-activate bad nodes by checking if they are back.
        // iterate all non-active nodes from node manager
        // ask connection pool to connect - if get valid channel, reactivate it
        ci.nodeManager().activate([](const nebula::meta::NNode& node) {
          return nebula::service::node::ConnectionPool::init()->test(node);
        });
      }

      // count number of times to update, log info every 500 updates
      if (++updates % 500 == 0) {
        LOG(INFO) << "Nebula Server updated itself " << updates << " times.";
      }
    });

  // node sync let it keep going using a unique spec repo per server
  // NOTE: our task scheduler schedule function run in sequence
  // interval is the time gap between any sequential two runs
  auto& pool = v1Service.pool();
  taskScheduler.setInterval(FLAGS_NODE_SYNC_INTERVAL, [&pool]() {
    nebula::service::server::NodeSync::sync(pool);
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
  folly::init(&argc, &argv);
  FLAGS_logtostderr = 1;

  RunServer();

  return 0;
}