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

#include "NebulaService.h"

#include <curl/curl.h>
#include <msgpack.hpp>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "NativeMetaDb.h"
#include "api/dsl/Serde.h"
#include "common/Evidence.h"
#include "common/Int128.h"
#include "execution/BlockManager.h"
#include "ingest/BlockExpire.h"
#include "memory/keyed/FlatRowCursor.h"
#include "meta/ClusterInfo.h"
#include "meta/TableSpec.h"
#include "meta/TestTable.h"
#include "storage/NFS.h"
#include "type/Serde.h"

/**
 * provide common data operations for service
 */
namespace nebula {
namespace service {
namespace base {

using nebula::api::dsl::Expression;
using nebula::api::dsl::Query;
using nebula::api::dsl::Serde;
using nebula::api::dsl::SortType;
using nebula::common::Pool;
using nebula::common::SingleCommandTask;
using nebula::common::Task;
using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::common::unordered_map;
using nebula::common::unordered_set;
using nebula::execution::PlanPtr;
using nebula::execution::QueryContext;
using nebula::execution::QueryStats;
using nebula::execution::QueryWindow;
using nebula::ingest::BlockExpire;
using nebula::ingest::IngestSpec;
using nebula::memory::keyed::FlatBuffer;
using nebula::memory::keyed::FlatRowCursor;
using nebula::meta::AccessSpec;
using nebula::meta::Column;
using nebula::meta::ColumnProps;
using nebula::meta::DataSource;
using nebula::meta::DataSpec;
using nebula::meta::DBType;
using nebula::meta::MetaConf;
using nebula::meta::MetaDb;
using nebula::meta::SpecState;
using nebula::meta::TableSpec;
using nebula::meta::TimeSpec;
using nebula::meta::TimeType;
using nebula::surface::EmptyRowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::type::Kind;
using nebula::type::Schema;

#define ERROR_MESSSAGE_CASE(ERROR)                 \
  case ErrorCode::ERROR: {                         \
    return ErrorTraits<ErrorCode::ERROR>::MESSAGE; \
  }

const std::string ServiceProperties::errorMessage(ErrorCode error) {
  switch (error) {
    ERROR_MESSSAGE_CASE(NONE)
    ERROR_MESSSAGE_CASE(MISSING_TABLE)
    ERROR_MESSSAGE_CASE(MISSING_TIME_RANGE)
    ERROR_MESSSAGE_CASE(MISSING_OUTPUT_FIELDS)
    ERROR_MESSSAGE_CASE(FAIL_BUILD_QUERY)
    ERROR_MESSSAGE_CASE(FAIL_COMPILE_QUERY)
    ERROR_MESSSAGE_CASE(FAIL_EXECUTE_QUERY)
    ERROR_MESSSAGE_CASE(AUTH_REQUIRED)
    ERROR_MESSSAGE_CASE(PERMISSION_REQUIRED)
    ERROR_MESSSAGE_CASE(TABLE_NOT_FOUND)
  default: throw NException("Error Code Not Covered");
  }
}
#undef ERROR_MESSSAGE_CASE

const std::string ServiceProperties::jsonify(const RowCursorPtr data, const Schema schema) {
  // set up JSON writer to serialize each row
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> json(buffer);

  // set up function callback to serialize each row with capture of the JSON writer
  std::vector<std::function<void(const RowData&)>> jsonCalls;
  auto numColumns = schema->size();
  jsonCalls.reserve(numColumns);
  for (size_t i = 0; i < numColumns; ++i) {
    auto type = schema->childType(i);
    const auto& name = type->name();

// define callback case for each type with gluing JSON function and row function
#define CALLBACK_CASE(K, JF, RF)                            \
  case Kind::K: {                                           \
    jsonCalls.push_back([name, &json](const RowData& row) { \
      json.Key(name);                                       \
      json.JF(row.RF(name));                                \
    });                                                     \
    break;                                                  \
  }

    switch (type->k()) {
      CALLBACK_CASE(BOOLEAN, Bool, readBool)
      CALLBACK_CASE(TINYINT, Int, readByte)
      CALLBACK_CASE(SMALLINT, Int, readShort)
      CALLBACK_CASE(INTEGER, Int, readInt)
      CALLBACK_CASE(REAL, Double, readFloat)
      CALLBACK_CASE(DOUBLE, Double, readDouble)
    case Kind::BIGINT: {
      // TODO(cao) - we need better serializeation format exhcanging with WEB
      // Due to JSON format on number - it can only have 16 significant digits
      // for any long value having more than that will be round to 0 causing precision problem.
      // So we serialize bigint into string
      jsonCalls.push_back([name, &json](const RowData& row) {
        json.Key(name);
        auto lv = std::to_string(row.readLong(name));
        json.String(lv.data(), lv.size());
      });
      break;
    }
    case Kind::INT128: {
      // TODO(cao) - we need better serializeation format exhcanging with WEB
      // Due to JSON format on number - it can only have 16 significant digits
      // for any long value having more than that will be round to 0 causing precision problem.
      // So we serialize bigint into string
      jsonCalls.push_back([name, &json](const RowData& row) {
        json.Key(name);
        auto lv = nebula::common::Int128_U::to_string(row.readInt128(name));
        json.String(lv.data(), lv.size());
      });
      break;
    }
    case Kind::VARCHAR: {
      jsonCalls.push_back([name, &json](const RowData& row) {
        json.Key(name);
        auto sv = row.readString(name);
        json.String(sv.data(), sv.size());
      });
      break;
    }
    default:
      throw NException("Json serialization not supporting this type yet");
    }

#undef CALLBACK_CASE
  }

  // all rows fit in an array
  json.StartArray();
  // serialize every single row
  while (data->hasNext()) {
    const auto& row = data->next();
    json.StartObject();

    for (auto& f : jsonCalls) {
      f(row);
    }

    json.EndObject();
  }

  json.EndArray();
  return buffer.GetString();
}

// serialize a query and meta data
flatbuffers::grpc::Message<QueryPlan> QuerySerde::serialize(const Query& q, const std::string& id, const QueryWindow& window) {
  flatbuffers::grpc::MessageBuilder mb;
  std::vector<flatbuffers::Offset<flatbuffers::String>> fields;
  fields.reserve(q.selects_.size());
  for (auto& f : q.selects_) {
    fields.push_back(mb.CreateString(Serde::serialize(*f)));
  }

  std::vector<uint32_t> groups;
  groups.reserve(q.groups_.size());
  for (auto i : q.groups_) {
    groups.push_back(i);
  }

  std::vector<uint32_t> sorts;
  sorts.reserve(q.sorts_.size());
  for (auto i : q.sorts_) {
    sorts.push_back(i);
  }

  auto tbl = q.table_->name();
  auto filter = Serde::serialize(*q.filter_);
  // customs serialization
  auto customs = Serde::serialize(q.customs_);
  auto request_offset = CreateQueryPlanDirect(
    mb, id.c_str(), tbl.c_str(), filter.c_str(), customs.c_str(), &fields, &groups, &sorts,
    q.sortType_ == SortType::DESC, q.limit_, window.first, window.second);
  mb.Finish(request_offset);
  return mb.ReleaseMessage<QueryPlan>();
}

// TODO(cao) - new fields are serialized by msgpack for simplicity such as "customs"
// consider to convert all other fields using msgpack instead dealing with complex types in flatbuffer
nebula::api::dsl::Query QuerySerde::deserialize(
  const std::shared_ptr<nebula::meta::MetaService> ms,
  const flatbuffers::grpc::Message<QueryPlan>* query) {
  auto plan = query->GetRoot();

  const auto table = flatbuffers::GetString(plan->tbl());
  Query q(table, ms);

  // set filter
  {
    q.filter_ = Serde::deserialize(plan->filter()->c_str());
  }

  // set customs
  {
    auto cc = plan->customs();
    q.customs_ = Serde::deserialize(cc->data(), cc->size());
  }

  // set fields
  {
    auto fs = plan->fields();
    auto size = fs->size();
    std::vector<std::shared_ptr<Expression>> fields;
    fields.reserve(size);
    for (uint32_t i = 0; i < size; ++i) {
      fields.push_back(Serde::deserialize(fs->Get(i)->c_str()));
    }
    q.selects_ = std::move(fields);
  }

  // set groups
  {
    auto gs = plan->groups();
    auto size = gs->size();
    std::vector<size_t> groups;
    groups.reserve(size);
    for (uint32_t i = 0; i < size; ++i) {
      groups.push_back(gs->Get(i));
    }
    q.groups_ = std::move(groups);
  }

  // set sorts
  {
    auto ss = plan->sorts();
    auto size = ss->size();
    std::vector<size_t> sorts;
    sorts.reserve(size);
    for (uint32_t i = 0; i < size; ++i) {
      sorts.push_back(ss->Get(i));
    }
    q.sorts_ = std::move(sorts);
  }

  // sort type
  q.sortType_ = plan->desc() ? SortType::DESC : SortType::ASC;

  // set limit
  q.limit_ = plan->limit();

  // return this deserialized query
  return q;
}

PlanPtr QuerySerde::from(Query& q, size_t start, size_t end) {
  // TODO(cao): serialize query context to nodes and mark compile method as const
  auto plan = q.compile(QueryContext::def());

  // set a few other properties associated with execution plan
  plan->setWindow({ start, end });

  // return this compiled plan
  return plan;
}

flatbuffers::grpc::Message<BatchRows> BatchSerde::serialize(const FlatBuffer& fb, const PlanPtr plan) {
  flatbuffers::grpc::MessageBuilder mb;
  auto schema = mb.CreateString(nebula::type::TypeSerializer::to(fb.schema()));
  int8_t* buffer;
  auto size = fb.prepareSerde();
  auto bytes = mb.CreateUninitializedVector<int8_t>(size, &buffer);
  fb.serialize(buffer);

  auto& stats = plan->ctx().stats();
  auto batch = CreateBatchRows(
    mb,
    schema,
    BatchType::BatchType_Flat,
    CreateStats(mb, stats.blocksScan, stats.rowsScan, stats.rowsRet),
    bytes);
  mb.Finish(batch);
  return mb.ReleaseMessage<BatchRows>();
}

RowCursorPtr BatchSerde::deserialize(const flatbuffers::grpc::Message<BatchRows>* batch,
                                     const nebula::surface::eval::Fields& fields,
                                     QueryStats& stats) {
  auto ptr = batch->GetRoot();

  const auto schema = nebula::type::TypeSerializer::from(flatbuffers::GetString(ptr->schema()));
  N_ENSURE(ptr->type() == BatchType::BatchType_Flat, "only support flat for now");

  // TODO(cao) - can we avoid this allocation?
  auto data = ptr->data();
  auto size = data->size();
  // short circuit of zero size data
  if (size == 0) {
    LOG(INFO) << "Received an empty result set.";
    return EmptyRowCursor::instance();
  }

  auto bytes = static_cast<NByte*>(Pool::getDefault().allocate(size));
  std::memcpy(bytes, data->data(), size);

  // get stats of this compute node - threadsafe?
  auto nodeStats = ptr->stats();
  stats.blocksScan += nodeStats->blocks_scan();
  stats.rowsScan += nodeStats->rows_scan();

  // TODO(cao) - It is not good, we're reference some data from batch but actually not owning it.
  auto fb = std::make_unique<FlatBuffer>(schema, fields, bytes);
  return std::make_shared<FlatRowCursor>(std::move(fb));
}

// serialize a ingest spec into a task spec to be sent over
flatbuffers::grpc::Message<TaskSpec> TaskSerde::serialize(const Task& task) {
  flatbuffers::grpc::MessageBuilder mb;

  // this is an ingestion type
  const auto type = task.type();
  const auto sync = task.sync();
  if (type == TaskType::INGESTION) {
    auto spec = task.spec<IngestSpec>();

    // create ingest task -
    // note that, we are dropping fields in IngestSpec which does the core work only
    auto strSpec = DataSpec::serialize(*spec);

    // serialize the ingest task
    auto it = CreateIngestTask(mb, mb.CreateString(strSpec));

    // create task spec
    auto ts = CreateTaskSpec(mb, type, sync, it);
    mb.Finish(ts);
    return mb.ReleaseMessage<TaskSpec>();
  }

  // serialize task of expiration
  if (type == TaskType::EXPIRATION) {
    auto spec = task.spec<BlockExpire>();

    // serialize expire task
    const auto& specs = spec->specs();
    std::vector<flatbuffers::Offset<Spec>> fbSpecs;
    fbSpecs.reserve(specs.size());
    for (auto& itr : specs) {
      fbSpecs.push_back(CreateSpec(mb, mb.CreateString(itr.first), mb.CreateString(itr.second)));
    }

    auto et = CreateExpireTask(mb, mb.CreateVector<flatbuffers::Offset<Spec>>(fbSpecs));

    // create task spec
    auto ts = CreateTaskSpec(mb, type, sync, 0, et);
    mb.Finish(ts);
    return mb.ReleaseMessage<TaskSpec>();
  }

  if (type == TaskType::COMMAND) {
    auto spec = task.spec<SingleCommandTask>();
    auto ct = CreateCommandTaskDirect(mb, spec->id().c_str());

    // create task spec
    auto ts = CreateTaskSpec(mb, type, sync, 0, 0, ct);
    mb.Finish(ts);
    return mb.ReleaseMessage<TaskSpec>();
  }

  throw NException(fmt::format("Unhandled task type: {0}", type));
}

// parse a task spec into an ingest spec
Task TaskSerde::deserialize(const flatbuffers::grpc::Message<TaskSpec>* ts) {
  auto ptr = ts->GetRoot();

  const auto tst = ptr->type();
  const auto type = static_cast<TaskType>(tst);
  const auto sync = ptr->sync();
  if (type == TaskType::INGESTION) {
    auto it = ptr->ingest();

    // NOTE: here incurs string copy in IngestSpec constructor, better to avoid it
    auto is = DataSpec::deserialize(it->spec()->str());
    return Task(type, is, sync);
  }

  if (type == TaskType::EXPIRATION) {
    auto expires = ptr->expire()->specs();
    unordered_set<std::pair<std::string, std::string>> specs;
    for (auto itr = expires->begin(); itr != expires->end(); ++itr) {
      specs.emplace(itr->tbl()->str(), itr->spec()->str());
    }

    // move this list into a new object
    auto be = std::make_shared<BlockExpire>(std::move(specs));
    return Task(type, be, sync);
  }

  if (type == TaskType::COMMAND) {
    std::string cmd = ptr->command()->command()->str();
    auto sct = std::make_shared<SingleCommandTask>(std::move(cmd));
    return Task(type, sct, sync);
  }

  throw NException(fmt::format("Unhandled task type: {0}", tst));
}

std::unique_ptr<MetaDb> createMetaDB(const MetaConf& conf) {
  if (conf.type == DBType::NONE) {
    return std::make_unique<nebula::meta::VoidDb>();
  }

  if (conf.type == DBType::NATIVE) {
    // generate a tmp path as local DB path
    return std::make_unique<NativeMetaDb>(conf.store);
  }

  throw NException("Not supported DB type");
}

void bestEffortDie() noexcept {
  nebula::meta::ClusterInfo::singleton().exit();
}

void globalInit() {
  // global init curl for usage of libcurl
  curl_global_init(CURL_GLOBAL_DEFAULT);
}

void globalExit() {
  // global init curl for usage of libcurl
  curl_global_cleanup();
}

} // namespace base
} // namespace service
} // namespace nebula