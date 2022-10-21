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

#include "LoadHandler.h"

#include <rapidjson/document.h>

#include "common/Evidence.h"
#include "common/Format.h"
#include "common/Hash.h"
#include "common/Params.h"
#include "common/Spark.h"
#include "execution/meta/TableService.h"
#include "meta/ClusterInfo.h"
#include "meta/Macro.h"
#include "meta/NNode.h"
#include "meta/TableSpec.h"
#include "service/base/GoogleSheet.h"
#include "service/base/LoadSpec.h"
#include "storage/NFS.h"

namespace nebula {
namespace service {
namespace server {

using nebula::common::Evidence;
using nebula::common::Hasher;
using nebula::common::ParamList;
using nebula::common::Spark;
using nebula::execution::meta::TableService;
using nebula::ingest::IngestSpec;
using nebula::meta::AccessSpec;
using nebula::meta::BlockSignature;
using nebula::meta::BucketInfo;
using nebula::meta::ClusterInfo;
using nebula::meta::ColumnProps;
using nebula::meta::CsvProps;
using nebula::meta::DataFormat;
using nebula::meta::DataFormatUtils;
using nebula::meta::DataSource;
using nebula::meta::JsonProps;
using nebula::meta::KafkaSerde;
using nebula::meta::Macro;
using nebula::meta::NNode;
using nebula::meta::RocksetSerde;
using nebula::meta::SpecSplit;
using nebula::meta::SpecSplitPtr;
using nebula::meta::SpecState;
using nebula::meta::TableSpec;
using nebula::meta::TableSpecPtr;
using nebula::meta::ThriftProps;
using nebula::meta::TTL;
using nebula::service::base::GoogleSheet;
using nebula::service::base::LoadSpec;
using nebula::type::Settings;

TableSpecPtr LoadHandler::loadConfigured(const LoadRequest* req, LoadError& err) {
  // get request content
  const auto& table = req->table();
  const auto& json = req->json();
  const auto ttl = req->ttl();

  // search the template from ClusterInfo
  auto& ci = ClusterInfo::singleton();
  TableSpecPtr tmp = nullptr;
  for (auto& ts : ci.tables()) {
    if (table == ts->name) {
      tmp = ts;
      break;
    }
  }

  if (tmp == nullptr) {
    err = LoadError::TEMPLATE_NOT_FOUND;
    return nullptr;
  }

  // based on parameters, we will generate a list of sources
  // we will generate a table instance based on the parameters
  // TODO(cao) - handle hash collision if cluster info already contains this table name
  size_t hash = Hasher::hashString(json);

  // all ephemeral table instance will start with a prefix
  // because normal table won't be able to configured through YAML with # since it's comment.
  const auto name = fmt::format("{0}{1}-{2}", BlockSignature::EPHEMERAL_TABLE_PREFIX, table, hash);

  // generate all the macro values for this table
  // by comparing the json value in table settings
  // std::map<std::string, std::vector<std::string>>
  std::map<std::string, std::vector<std::string>> macroValues;
  rapidjson::Document doc;
  if (doc.Parse(json.c_str()).HasParseError()) {
    LOG(WARNING) << fmt::format("Error parsing params-json: {0}", json);
    err = LoadError::PARSE_ERROR;
    return nullptr;
  }

  // convert this into a param list
  N_ENSURE(doc.IsObject(), "expect params-json as an object.");
  auto obj = doc.GetObject();
  for (auto& m : obj) {
    std::vector<std::string> values;
    if (m.value.IsArray()) {
      auto a = m.value.GetArray();
      values.reserve(a.Size());
      for (auto& v : a) {
        values.push_back(v.GetString());
      }
    } else {
      values.reserve(1);
      values.push_back(m.value.GetString());
    }

    macroValues.emplace(m.name.GetString(), values);
  }

  // enroll this table in table service
  auto tbSpec = std::make_shared<TableSpec>(
    name,
    tmp->max_mb,
    ttl,
    tmp->schema,
    tmp->source,
    tmp->loader,
    tmp->location,
    tmp->backup,
    tmp->format,
    tmp->csv,
    tmp->json,
    tmp->thrift,
    tmp->kafkaSerde,
    tmp->rocksetSerde,
    tmp->columnProps,
    tmp->timeSpec,
    tmp->accessSpec,
    tmp->bucketInfo,
    tmp->settings,
    macroValues,
    tmp->headers,
    tmp->optimalBlockSize);

  // must enroll this new dataset to make it visible to client
  tbSpec->ttl = TTL(ttl);
  return tbSpec;
}

// load a google sheet request as specs
// it could be mulitple specs if we split single sheets by row range for large file.
TableSpecPtr LoadHandler::loadGoogleSheet(const LoadRequest* req, LoadError& err) {
  // public data source will be under the default namespace, probably called [PUBLIC]
  // use passed in name as data source name, API loaded table needs to be started with #
  const auto name = fmt::format("{0}{1}", BlockSignature::EPHEMERAL_TABLE_PREFIX, req->table());

  // TTL in seconds
  const auto ttl = req->ttl();

  // google sheet spec in json format.
  const auto& json = req->json();
  // by comparing the json value in table settings
  rapidjson::Document doc;
  if (doc.Parse(json.c_str()).HasParseError()) {
    LOG(WARNING) << "Error parsing google sheet json: " << json;
    err = LoadError::PARSE_ERROR;
    return nullptr;
  }

  GoogleSheet sheet{ doc };

  // build a table spec
  auto tbSpec = std::make_shared<TableSpec>(
    name,
    GoogleSheet::MAX_SIZE_MB,
    ttl,
    sheet.schema,
    DataSource::GSHEET,
    GoogleSheet::LOADER,
    sheet.url,
    GoogleSheet::BACKUP,
    DataFormat::GSHEET,
    CsvProps{},
    JsonProps{},
    ThriftProps{},
    KafkaSerde{},
    RocksetSerde{},
    ColumnProps{},
    sheet.timeSpec,
    sheet.accessSpec,
    BucketInfo::empty(),
    sheet.settings,
    std::map<std::string, std::vector<std::string>>(),
    std::vector<std::string>(),
    0);

  // set ttl of this ephemeral table
  tbSpec->ttl = TTL(ttl);
  return tbSpec;
}

// auto detect the schema if not provided
TableSpecPtr LoadHandler::loadDemand(const LoadRequest* req, LoadError& err) {
  const auto& table = req->table();

  // TTL in seconds
  const auto ttl = req->ttl();

  // configuration in json format
  const auto& json = req->json();

  // cluster info manages all table definitions
  auto& ci = ClusterInfo::singleton();

  // by comparing the json value in table settings
  rapidjson::Document doc;
  if (doc.Parse(json.c_str()).HasParseError()) {
    LOG(WARNING) << "Error parsing load spec json: " << json;
    err = LoadError::PARSE_ERROR;
    return nullptr;
  }

  N_ENSURE(doc.IsObject(), "json object expected in google sheet spec.");

  // TODO: update the spec to explicitly call it out
  // Here - we allow client to add a new table definition in yaml format
  // the same table definition format supported in cluster.yaml
  // when ttl is 0, we trigger this use case.
  if (ttl == 0) {
    LOG(INFO) << "Accepting a new table definition in yaml: " << table;
    std::string yaml = "";
    const auto root = doc.GetObject();
    auto member = root.FindMember("yaml");
    if (member != root.MemberEnd()) {
      yaml = member->value.GetString();
    }

    if (yaml.size() == 0) {
      LOG(ERROR) << "Invalid yaml, expect `{\"yaml\": \"<table definition>\"}`";
    } else {
      // add a new table entry
      auto error = ci.addTable(table, yaml);
      if (error.size() > 0) {
        LOG(ERROR) << "Failed to add this new table: " << error;
      }
    }

    // stop here for any request with TTL equals to 0
    return nullptr;
  }

  // get table name - needs to be unique for all other ephmeral table
  const auto name = fmt::format("{0}{1}", BlockSignature::EPHEMERAL_TABLE_PREFIX, table);
  LoadSpec demand{ doc };

  // build a table spec
  auto format = DataFormatUtils::from(demand.format);
  if (format == DataFormat::UNKNOWN) {
    LOG(ERROR) << "Data format is unknown: " << demand.format;
    err = LoadError::NOT_SUPPORTED;
    return nullptr;
  }

  auto tbSpec = std::make_shared<TableSpec>(
    name,
    LoadSpec::MAX_SIZE_MB,
    ttl,
    demand.schema,
    demand.source,
    LoadSpec::LOADER,
    demand.path,
    LoadSpec::BACKUP,
    format,
    demand.csv,
    demand.json,
    demand.thrift,
    KafkaSerde{},
    RocksetSerde{},
    demand.props,
    demand.timeSpec,
    demand.accessSpec,
    BucketInfo::empty(),
    demand.settings,
    demand.macros,
    demand.headers,
    demand.optimalBlockSize);

  // set ttl of this ephemeral table
  tbSpec->ttl = TTL(ttl);
  return tbSpec;
}

} // namespace server
} // namespace service
} // namespace nebula