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

#include "NebulaServer.h"

#include <cstdlib>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <thread>
#include <unordered_map>

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
#include "ingest/SpecRepo.h"
#include "memory/Batch.h"
#include "meta/ClusterInfo.h"
#include "meta/NBlock.h"
#include "meta/NNode.h"
#include "meta/TableSpec.h"
#include "nebula.grpc.pb.h"
#include "service/base/NebulaService.h"
#include "service/node/RemoteNodeConnector.h"
#include "storage/NFS.h"
#include "storage/NFileSystem.h"

// use "host.docker.internal" for docker env
DEFINE_string(CLS_CONF, "configs/cluster.yml", "cluster config file");
DEFINE_uint64(CLS_CONF_UPDATE_INTERVAL, 5000, "interval in milliseconds to update cluster config");
DEFINE_uint64(NODE_SYNC_INTERVAL, 5000, "interval in ms to conduct node sync");
DEFINE_uint32(MAX_TABLES_RETURN, 500, "max tables to fetch to display");

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
using nebula::api::dsl::QueryContext;
using nebula::common::Evidence;
using nebula::common::ParamList;
using nebula::common::SingleCommandTask;
using nebula::common::Task;
using nebula::common::TaskType;
using nebula::execution::BlockManager;
using nebula::execution::io::BlockLoader;
using nebula::execution::meta::TableService;
using nebula::ingest::IngestSpec;
using nebula::ingest::SpecState;
using nebula::memory::Batch;
using nebula::meta::BlockSignature;
using nebula::meta::Table;
using nebula::meta::TableSpec;
using nebula::meta::TableSpecPtr;
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

  auto table = TableService::singleton()->query(tbl).table();
  auto bm = BlockManager::init();
  // query the table's state
  auto metrics = bm->metrics(table->name());
  reply->set_blockcount(metrics.numBlocks());
  reply->set_rowcount(metrics.numRows());
  reply->set_memsize(metrics.rawBytes());
  auto window = metrics.timeWindow();
  reply->set_mintime(window.first);
  reply->set_maxtime(window.second);

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
    auto nodes = nebula::meta::ClusterInfo::singleton().nodes();
    N_ENSURE(nodes.size() > 0, "cluster info has no nodes??");
    auto client = connector.makeClient(*nodes.begin(), threadPool_);
    Task task(TaskType::COMMAND, SingleCommandTask::shutdown());
    client->task(task);
    return Status::OK;
  }

  LOG(WARNING) << "Required message to be correct to shutdown single working node";
  return Status::CANCELLED;
}

QueryContext buildQueryContext(ServerContext* ctx) {
  // build query context
  const auto& metadata = ctx->client_metadata();

  // fetch user info keys, refer userInfo definition in node.js serving http traffic
  // auth, authorization, user, groups
  std::string user{ "unauth" };
  std::unordered_set<std::string> groups;
  auto itr = metadata.find("nebula-auth");
  if (itr != metadata.end() && itr->second == "1") {
    auto u = metadata.find("nebula-user");
    if (u != metadata.end()) {
      user = std::string(u->second.data(), u->second.size());
    }

    auto g = metadata.find("nebula-groups");
    if (g != metadata.end()) {
      groups = nebula::common::Chars::split(g->second.data(), g->second.size());
    }
  }

  return QueryContext{ user, std::move(groups) };
}

Status V1ServiceImpl::Load(ServerContext* ctx, const LoadRequest* req, LoadResponse* reply) {
  nebula::common::Evidence::Duration tick;
  // get query context
  auto queryContext = buildQueryContext(ctx);
  LOG(INFO) << "Load data source for user: " << queryContext.user();

  // get request content
  const auto& table = req->table();
  const auto& paramsJson = req->paramsjson();
  // STL = TTL in seconds
  const auto stl = req->ttl();
  const auto ttlHour = stl / 3600;

  // search the template from ClusterInfo
  auto& ci = nebula::meta::ClusterInfo::singleton();
  TableSpecPtr tmp = nullptr;
  for (auto& ts : ci.tables()) {
    if (table == ts->name) {
      tmp = ts;
      break;
    }
  }

  if (tmp == nullptr) {
    reply->set_error(LoadError::TEMPLATE_NOT_FOUND);
    return Status::CANCELLED;
  }

  // based on parameters, we will generate a list of sources
  // we will generate a table instance based on the parameters
  // TODO(cao) - handle hash collision if cluster info already contains this table name
  size_t hash = nebula::common::Hasher::hashString(paramsJson);
  // all ephemeral table instance will start with "#"
  // because normal table won't be able to configured through YAML with # since it's comment.
  auto tableName = fmt::format("{0}{1}-{2}", BlockSignature::EPHEMERAL_TABLE_PREFIX, table, hash);
  // enroll this table in table service
  // nebula::execution::meta::TableService::singleton()->enroll
  auto tbSpec = std::make_shared<TableSpec>(
    tableName,
    tmp->max_mb,
    ttlHour,
    tmp->schema,
    tmp->source,
    tmp->loader,
    tmp->location,
    tmp->backup,
    tmp->format,
    tmp->serde,
    tmp->columnProps,
    tmp->timeSpec,
    tmp->accessSpec,
    tmp->bucketInfo,
    tmp->settings);

  // pattern must be present in settings
  nebula::execution::meta::TableService::singleton()->enroll(tbSpec->to(), stl);

  // by comparing the paramsJson value in table settings
  rapidjson::Document cd;
  if (cd.Parse(paramsJson.c_str()).HasParseError()) {
    throw NException(fmt::format("Error parsing params-json: {0}", paramsJson));
  }

  // convert this into a param list
  N_ENSURE(cd.IsObject(), "expect params-json as an object.");
  ParamList params(cd);

  // reserved macro "date" is used to recognize current date value for any placeholder.
  static constexpr auto DATE = "date";
  static constexpr auto BUCKET = "bucket";

  auto sourceInfo = nebula::storage::parse(tmp->location);
  // for every single combination, we will use it to format template source to get a new spec source
  std::vector<std::shared_ptr<IngestSpec>> specs;
  auto p = params.next();
  auto& nodeSet = ci.nodes();
  std::vector<nebula::meta::NNode> nodes(nodeSet.begin(), nodeSet.end());
  size_t assignId = 0;
  while (p.size() > 0) {
    // get date info if provided by parameters
    auto d = p.find(DATE);
    auto dateMs = 0;
    if (d != p.end()) {
      dateMs = nebula::common::Evidence::time(p.at(DATE), "%Y-%m-%d");
    }

    // if bucket is required, bucket column value must be provided
    // but if bucket parameter is provided directly, we don't need to compute
    auto bucketCount = tbSpec->bucketInfo.count;
    if (p.find(BUCKET) == p.end() && bucketCount > 0) {
      // TODO(cao) - short term dependency, if Spark changes its hash algo, this will be broken
      auto bcValue = folly::to<size_t>(p.at(tbSpec->bucketInfo.bucketColumn));
      auto bucket = std::to_string(nebula::common::Spark::hashBucket(bcValue, bucketCount));

      // TODO(cao) - ugly code, how can we avoid this?
      auto width = std::log10(bucketCount) + 1;
      bucket = std::string((width - bucket.size()), '0').append(bucket);
      p.emplace(BUCKET, bucket);
    }

    // if the table has bucket info
    auto path = nebula::common::format(sourceInfo.path, p);
    LOG(INFO) << "Generate a spec path: " << path;
    // build ingestion spec from this location
    auto spec = std::make_shared<IngestSpec>(tbSpec, "0", path, sourceInfo.host, 0, SpecState::NEW, dateMs);

    // round robin assign the spec to each node
    spec->setAffinity(nodes.at(assignId));
    if (++assignId >= nodes.size()) {
      assignId = 0;
    }

    specs.push_back(spec);

    // see next params
    p = params.next();
  }

  // now we have a list of specs, let's assign nodes to these ingest specs and send to each node
  auto& threadPool = pool();
  auto connector = std::make_shared<node::RemoteNodeConnector>(nullptr);
  std::vector<folly::Future<bool>> futures;
  futures.reserve(specs.size());
  for (auto spec : specs) {
    auto promise = std::make_shared<folly::Promise<bool>>();

    // pass values since we reutrn the whole lambda - don't reference temporary things
    // such as local stack allocated variables, including "this" the client itself.
    threadPool.add([promise, connector, spec, &threadPool]() {
      auto client = connector->makeClient(spec->affinity(), threadPool);
      // build a task out of this spec
      Task t(TaskType::INGESTION, std::static_pointer_cast<nebula::common::Identifiable>(spec), true);
      nebula::common::TaskState state = client->task(t);
      if (state == nebula::common::TaskState::SUCCEEDED) {
        spec->setState(SpecState::READY);
        promise->setValue(true);
        return;
      }

      // else return empty result set
      promise->setValue(false);
    });

    futures.push_back(promise->getFuture());
  }

  auto x = folly::collectAll(futures).get();
  auto succeeded = true;
  for (auto it = x.begin(); it < x.end(); ++it) {
    // if the result is empty
    if (!it->hasValue() || !it->value()) {
      succeeded = false;
      break;
    }
  }

  if (succeeded) {
    reply->set_error(LoadError::SUCCESS);
    reply->set_loadtimems(tick.elapsedMs());
    reply->set_table(tableName);
    return Status::OK;
  }

  reply->set_error(LoadError::TEMPLATE_NOT_FOUND);
  return Status::CANCELLED;
}

Status V1ServiceImpl::Query(ServerContext* ctx, const QueryRequest* request, QueryResponse* reply) {
  // validate the query request and build the call
  nebula::common::Evidence::Duration tick;
  ErrorCode error = ErrorCode::NONE;

  auto tableName = request->table();

  // get the table registry and activate it by recording latest used time
  auto tr = TableService::singleton()->query(tableName);
  tr.activate();

  // build the query
  auto query = handler_.build(*tr.table(), *request, error);
  if (error != ErrorCode::NONE) {
    return replyError(error, reply, 0);
  }

  // get query context
  auto queryContext = buildQueryContext(ctx);

  // compile query into a query plan
  auto plan = handler_.compile(
    query, { request->start(), request->end() }, queryContext, error);
  if (error != ErrorCode::NONE) {
    return replyError(error, reply, 0);
  }

  // create a remote connector and execute the query plan
  auto connector = std::make_shared<RemoteNodeConnector>(query);
  RowCursorPtr result = handler_.query(threadPool_, *plan, connector, error);
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

Status V1ServiceImpl::replyError(ErrorCode code, QueryResponse* reply, size_t durationMs) const {
  N_ENSURE_NE(code, ErrorCode::NONE, "Error Reply Code Not 0");

  auto error = ServiceProperties::errorMessage(code);
  auto stats = reply->mutable_stats();
  stats->set_error(code);
  stats->set_message(error);
  stats->set_querytimems(durationMs);

  return Status(StatusCode::INTERNAL, error);
}

// Logic and data behind the server's behavior.
class EchoServiceImpl final : public Echo::Service {
  Status EchoBack(ServerContext*, const EchoRequest* request, EchoResponse* reply) override {
    std::string prefix("This is from nebula: ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }
};

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

// NOTE: main function can't be placed inside a namespace
// Otherwise you may get undefined symbol "_main" error in link
void RunServer() {
  std::string server_address(fmt::format("0.0.0.0:{0}", nebula::service::base::ServiceProperties::PORT));
  nebula::service::server::EchoServiceImpl echoService;
  nebula::service::server::V1ServiceImpl v1Service;

  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&echoService).RegisterService(&v1Service);
  // set compression level as medium
  builder.SetDefaultCompressionLevel(GRPC_COMPRESS_LEVEL_MED);
  // Finally assemble the server.
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG(INFO) << "Nebula server listening on " << server_address;

  // a unique spec repo per server
  nebula::ingest::SpecRepo specRepo;

  // start node sync to sync all node's states and tasks
  // auto nsync = nebula::service::server::NodeSync::async(v1Service.pool(), specRepo, FLAGS_NODE_SYNC_INTERVAL);

  // TODO (cao): start a thread to sync up with etcd setup for cluster info.
  // register cluster info, we're using two different time based scheduelr currently
  // one is NodeSync uses folly::FunctionScheduler and TaskScheduler is built on top of EventBase
  // We're using task scheduler to pull all config changes and spec generation, assignment
  // NodeSync will be responsible to pull all state info from each Node and update them in server
  nebula::common::TaskScheduler taskScheduler;

  // having a local file system to detect change of cluster config
  auto& pool = v1Service.pool();
  std::string confSignature;
  taskScheduler.setInterval(
    FLAGS_CLS_CONF_UPDATE_INTERVAL,
    [&pool, &specRepo, &confSignature] {
      // load config into cluster info
      auto conf = LoadClusterConfig();

      // if the conf is a S3 file, download it and replace the conf as the local copy
      auto uri = nebula::storage::parse(conf);
      bool copied = false;
      if (uri.schema == "s3") {
        // create a s3 file system
        auto fs = nebula::storage::makeFS("s3", uri.host);
        conf = fs->copy(uri.path);
        copied = true;
      }

      // assuming everything else are local file - if not, let runtime fails you
      auto fs = nebula::storage::makeFS("local");
      auto fi = fs->info(conf);
      auto sign = fi.signature();

      // for copied data, they will have random name and latest timestamp
      // so we use size and content hash as its signature
      if (copied) {
        const auto size = fi.size;
        auto data = std::make_unique<char[]>(size);
        N_ENSURE_EQ(fs->read(conf, data.get()), size, "should read all bytes of the conf file");
        sign = fmt::format("{0}_{1}", size, nebula::common::Hasher::hash64(data.get(), size));
      }

      if (sign != confSignature) {
        LOG(INFO) << "Loading nebula cluster config: " << conf;

        // update the sign
        confSignature = sign;

        // load the new conf
        auto& ci = nebula::meta::ClusterInfo::singleton();
        ci.load(conf);

        // TODO(cao) - how to support table schema/column props evolution?
        nebula::execution::meta::TableService::singleton()->enroll(ci);
      }

      // if the file is copied tmp file, delete it
      if (copied) {
        unlink(conf.c_str());
      }

      {
        // sync cluster state
        nebula::service::server::NodeSync::sync(pool, specRepo);
      }
    });

  // NOTE that, this is blocking main thread to wait for server down
  // this may prevent system to exit properly, will revisit and revise.
  // run the loop.
  taskScheduler.run();

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();

  // shut down node sync process
  // nsync->shutdown();
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  FLAGS_logtostderr = 1;

  RunServer();

  return 0;
}