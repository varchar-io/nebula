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

#include "NebulaService.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "api/dsl/Serde.h"
#include "common/Evidence.h"
#include "common/Int128.h"
#include "execution/BlockManager.h"
#include "execution/ExecutionPlan.h"
#include "ingest/BlockExpire.h"
#include "memory/keyed/FlatRowCursor.h"
#include "meta/TableSpec.h"
#include "meta/TestTable.h"
#include "type/Serde.h"

/**
 * provide common data operations for service
 */
namespace nebula {
namespace service {
namespace base {

using nebula::api::dsl::Expression;
using nebula::api::dsl::Query;
using nebula::api::dsl::QueryContext;
using nebula::api::dsl::Serde;
using nebula::api::dsl::SortType;
using nebula::common::Pool;
using nebula::common::SingleCommandTask;
using nebula::common::Task;
using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::execution::QueryWindow;
using nebula::ingest::BlockExpire;
using nebula::ingest::IngestSpec;
using nebula::ingest::SpecState;
using nebula::memory::keyed::FlatBuffer;
using nebula::memory::keyed::FlatRowCursor;
using nebula::meta::AccessSpec;
using nebula::meta::Column;
using nebula::meta::ColumnProps;
using nebula::meta::DataSource;
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
  auto tbl = q.table_->name();
  auto filter = Serde::serialize(*q.filter_);
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

  auto request_offset = CreateQueryPlanDirect(
    mb, id.c_str(), tbl.c_str(), filter.c_str(), &fields, &groups, &sorts,
    q.sortType_ == SortType::DESC, q.limit_, window.first, window.second);
  mb.Finish(request_offset);
  return mb.ReleaseMessage<QueryPlan>();
}

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

std::unique_ptr<nebula::execution::ExecutionPlan> QuerySerde::from(
  const std::shared_ptr<nebula::meta::MetaService> ms,
  const flatbuffers::grpc::Message<QueryPlan>* msg) {
  auto query = QuerySerde::deserialize(ms, msg);

  // TODO(cao): serialize query context to nodes and mark compile method as const
  QueryContext ctx{ "nebula", { "nebula-users" } };
  auto plan = query.compile(ctx);

  // set a few other properties associated with execution plan
  auto p = msg->GetRoot();
  plan->setWindow({ p->tstart(), p->tend() });

  // return this compiled plan
  return plan;
}

flatbuffers::grpc::Message<BatchRows> BatchSerde::serialize(const FlatBuffer& fb) {
  flatbuffers::grpc::MessageBuilder mb;
  auto schema = mb.CreateString(nebula::type::TypeSerializer::to(fb.schema()));
  int8_t* buffer;
  auto size = fb.prepareSerde();
  auto bytes = mb.CreateUninitializedVector<int8_t>(size, &buffer);
  fb.serialize(buffer);

  auto batch = CreateBatchRows(mb, schema, BatchType::BatchType_Flat, bytes);
  mb.Finish(batch);
  return mb.ReleaseMessage<BatchRows>();
}

RowCursorPtr BatchSerde::deserialize(const flatbuffers::grpc::Message<BatchRows>* batch,
                                     const nebula::surface::eval::Fields& fields) {
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

  // TODO(cao) - It is not good, we're reference some data from batch but actually not owning it.
  auto fb = std::make_unique<FlatBuffer>(schema, fields, bytes);
  return std::make_shared<FlatRowCursor>(std::move(fb));
}

// serialize a ingest spec into a task spec to be sent over
flatbuffers::grpc::Message<TaskSpec> TaskSerde::serialize(const Task& task) {
  flatbuffers::grpc::MessageBuilder mb;

  // this is an ingestion type
  const auto tt = task.type();
  if (tt == TaskType::INGESTION) {
    auto spec = task.spec<IngestSpec>();

    // create ingest task
    auto table = spec->table();
    const auto& time = table->timeSpec;

    // serialize serde
    auto mapSize = table->serde.cmap.size();
    std::vector<flatbuffers::Offset<ColumnMap>> cmap;
    cmap.reserve(mapSize);
    for (auto& itr : table->serde.cmap) {
      cmap.push_back(
        CreateColumnMap(mb, mb.CreateString(itr.first), itr.second));
    }
    auto serde = CreateSerde(mb,
                             mb.CreateString(table->serde.protocol),
                             mb.CreateVector<flatbuffers::Offset<ColumnMap>>(cmap));

    // serialize column properties
    auto numCols = table->columnProps.size();
    std::vector<flatbuffers::Offset<ColumnProp>> colProps;
    colProps.reserve(numCols);
    for (auto& itr : table->columnProps) {
      auto& cp = itr.second;
      // if the column has partition info, we will need to serialize it
      flatbuffers::Offset<PartitionInfo> pi;
      if (cp.partition.valid()) {
        const auto& values = cp.partition.values;
        std::vector<flatbuffers::Offset<flatbuffers::String>> fbValues;
        fbValues.reserve(values.size());
        for (auto& v : values) {
          fbValues.emplace_back(mb.CreateString(v));
        }

        pi = CreatePartitionInfo(mb, mb.CreateVector<flatbuffers::Offset<flatbuffers::String>>(fbValues), cp.partition.chunk);
      }

      colProps.push_back(
        CreateColumnProp(mb, mb.CreateString(itr.first),
                         cp.withBloomFilter,
                         cp.withDict,
                         cp.withCompress,
                         mb.CreateString(cp.defaultValue),
                         pi));
    }
    auto fbColProps = mb.CreateVector<flatbuffers::Offset<ColumnProp>>(colProps);

    // serialize the ingest task
    auto it = CreateIngestTask(mb,
                               mb.CreateString(table->name),
                               table->max_hr,
                               mb.CreateString(table->loader),
                               (int8_t)table->source,
                               mb.CreateString(table->location),
                               mb.CreateString(table->format),
                               mb.CreateString(table->schema),
                               (int8_t)time.type,
                               time.unixTimeValue,
                               mb.CreateString(time.colName),
                               mb.CreateString(time.pattern),
                               serde,
                               fbColProps,
                               mb.CreateString(spec->version()),
                               mb.CreateString(spec->id()),
                               mb.CreateString(spec->domain()),
                               spec->size(),
                               (int8_t)spec->state(),
                               spec->macroDate());

    // create task spec
    auto ts = CreateTaskSpec(mb, tt, it);
    mb.Finish(ts);
    return mb.ReleaseMessage<TaskSpec>();
  }

  // serialize task of expiration
  if (tt == TaskType::EXPIRATION) {
    auto spec = task.spec<BlockExpire>();

    // create ingest task
    auto blocks = spec->blocks();
    std::vector<flatbuffers::Offset<flatbuffers::String>> fbBlocks;
    fbBlocks.reserve(blocks.size());
    for (auto& itr : blocks) {
      fbBlocks.push_back(mb.CreateString(itr));
    }

    auto et = CreateExpireTask(mb, mb.CreateVector<flatbuffers::Offset<flatbuffers::String>>(fbBlocks));

    // create task spec
    auto ts = CreateTaskSpec(mb, tt, 0, et);
    mb.Finish(ts);
    return mb.ReleaseMessage<TaskSpec>();
  }

  if (tt == TaskType::COMMAND) {
    auto spec = task.spec<SingleCommandTask>();
    auto ct = CreateCommandTaskDirect(mb, spec->signature().c_str());

    // create task spec
    auto ts = CreateTaskSpec(mb, tt, 0, 0, ct);
    mb.Finish(ts);
    return mb.ReleaseMessage<TaskSpec>();
  }

  throw NException(fmt::format("Unhandled task type: {0}", tt));
}

// parse a task spec into an ingest spec
Task TaskSerde::deserialize(const flatbuffers::grpc::Message<TaskSpec>* ts) {
  auto ptr = ts->GetRoot();

  const auto tst = ptr->type();
  auto tt = static_cast<TaskType>(tst);
  if (tt == TaskType::INGESTION) {
    auto it = ptr->ingest();

    // NOTE: here incurs string copy in IngestSpec constructor, better to avoid it
    // build time spec
    TimeSpec time{
      (TimeType)it->time_type(),
      it->time_value(),
      it->time_column()->str(),
      it->time_format()->str()
    };

    // build column properties
    ColumnProps props;
    auto colProps = it->column_props();
    for (auto itr = colProps->begin(); itr != colProps->end(); ++itr) {
      // adding all properties here
      nebula::meta::PartitionInfo partInfo;
      auto pi = itr->pi();
      if (pi) {
        partInfo.chunk = pi->chunk();
        auto values = pi->values();
        partInfo.values.reserve(values->size());
        for (auto itr = values->begin(); itr != values->end(); ++itr) {
          partInfo.values.emplace_back(itr->data(), itr->size());
        }
      }

      props[itr->name()->str()] = Column{
        itr->bf(),
        itr->dict(),
        itr->comp(),
        itr->dv()->str(),
        {},
        std::move(partInfo)
      };
    }

    // build access rules
    // TODO(cao): currently we don't serde acccess spec to nodes
    // because all access control happens at server right now.
    // This behavior may change in the future when a dedicated node assigned to client for streaming.
    AccessSpec as;

    // build serde
    auto serde = it->serde();
    nebula::meta::KafkaSerde sd;
    sd.protocol = serde->protocol()->str();
    auto cmap = serde->column_map();
    for (auto itr = cmap->begin(); itr != cmap->end(); ++itr) {
      sd.cmap.emplace(itr->name()->str(), itr->index());
    }

    // TODO(cao): settings needs to be sync to all nodes as it may contain critical info
    // such as SSL cert settings to enable ingester read data properly
    // build key-value settings
    std::unordered_map<std::string, std::string> settings;

    // build table spec
    std::string tbName = it->name()->str();
    std::string src = it->location()->str();
    std::string bak = "";
    auto table = std::make_shared<TableSpec>(std::move(tbName),
                                             0,
                                             it->max_hr(),
                                             it->schema()->str(),
                                             (DataSource)it->source(),
                                             it->loader()->str(),
                                             std::move(src),
                                             std::move(bak),
                                             it->format()->str(),
                                             std::move(sd),
                                             std::move(props),
                                             std::move(time),
                                             std::move(as),
                                             std::move(settings));

    auto is = std::make_shared<IngestSpec>(
      table,
      it->version()->str(),
      it->id()->str(),
      it->domain()->str(),
      it->size(),
      (SpecState)it->state(),
      it->date());

    return Task(tt, is);
  }

  if (tt == TaskType::EXPIRATION) {
    auto blocks = ptr->expire()->blocks();
    std::list<std::string> idList;
    for (auto itr = blocks->begin(); itr != blocks->end(); ++itr) {
      idList.emplace_back(itr->data(), itr->size());
    }

    // move this list into a new object
    auto be = std::make_shared<BlockExpire>(std::move(idList));
    return Task(tt, be);
  }

  if (tt == TaskType::COMMAND) {
    std::string cmd = ptr->command()->command()->str();
    auto sct = std::make_shared<SingleCommandTask>(std::move(cmd));
    return Task(tt, sct);
  }

  throw NException(fmt::format("Unhandled task type: {0}", tst));
}

} // namespace base
} // namespace service
} // namespace nebula