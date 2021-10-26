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
using nebula::ingest::SpecState;
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
using nebula::meta::TableSpec;
using nebula::meta::TableSpecPtr;
using nebula::meta::ThriftProps;
using nebula::service::base::GoogleSheet;
using nebula::service::base::LoadSpec;
using nebula::type::Settings;

LoadResult LoadHandler::loadConfigured(const LoadRequest* req, LoadError& err, std::string& name) {
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
    return {};
  }

  // based on parameters, we will generate a list of sources
  // we will generate a table instance based on the parameters
  // TODO(cao) - handle hash collision if cluster info already contains this table name
  size_t hash = Hasher::hashString(json);
  // all ephemeral table instance will start with "#"
  // because normal table won't be able to configured through YAML with # since it's comment.
  name = fmt::format("{0}{1}-{2}", BlockSignature::EPHEMERAL_TABLE_PREFIX, table, hash);
  // enroll this table in table service
  // nebula::execution::meta::TableService::singleton()->enroll
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
    tmp->columnProps,
    tmp->timeSpec,
    tmp->accessSpec,
    tmp->bucketInfo,
    tmp->settings);

  // must enroll this new dataset to make it visible to client
  TableService::singleton()->enroll(tbSpec->to(), ttl);

  // by comparing the json value in table settings
  rapidjson::Document cd;
  if (cd.Parse(json.c_str()).HasParseError()) {
    throw NException(fmt::format("Error parsing params-json: {0}", json));
  }

  // convert this into a param list
  N_ENSURE(cd.IsObject(), "expect params-json as an object.");
  ParamList params(cd);
  auto p = params.next();
  static constexpr auto BUCKET = "bucket";

  auto sourceInfo = nebula::storage::parse(tmp->location);
  // for every single combination, we will use it to format template source to get a new spec source
  LoadResult specs;
  auto& nodeSet = ci.nodes();
  std::vector<NNode> nodes(nodeSet.begin(), nodeSet.end());
  size_t assignId = 0;
  while (p.size() > 0) {
    // get date info if provided by parameters
    auto watermark = Macro::watermark(p);

    // if bucket is required, bucket column value must be provided
    // but if bucket parameter is provided directly, we don't need to compute
    auto bucketCount = tbSpec->bucketInfo.count;
    if (p.find(BUCKET) == p.end() && bucketCount > 0) {
      // TODO(cao) - short term dependency, if Spark changes its hash algo, this will be broken
      auto bcValue = folly::to<size_t>(p.at(tbSpec->bucketInfo.bucketColumn));
      auto bucket = std::to_string(Spark::hashBucket(bcValue, bucketCount));

      // TODO(cao) - ugly code, how can we avoid this?
      auto width = std::log10(bucketCount) + 1;
      bucket = std::string((width - bucket.size()), '0').append(bucket);
      p.emplace(BUCKET, bucket);
    }

    // if the table has bucket info
    auto path = nebula::common::format(sourceInfo.path, p);
    LOG(INFO) << "Generate a spec path: " << path;
    // build ingestion spec from this location
    auto spec = std::make_shared<IngestSpec>(tbSpec, "0", path, sourceInfo.host, 0, SpecState::NEW, watermark);

    // round robin assign the spec to each node
    spec->setAffinity(nodes.at(assignId));
    if (++assignId >= nodes.size()) {
      assignId = 0;
    }

    specs.push_back(spec);

    // see next params
    p = params.next();
  }

  // possible we got an empty result
  if (specs.empty()) {
    err = LoadError::EMPTY_RESULT;
  }

  return specs;
}

// load a google sheet request as specs
// it could be mulitple specs if we split single sheets by row range for large file.
LoadResult LoadHandler::loadGoogleSheet(const LoadRequest* req, LoadError& err, std::string& name) {
  // TODO(cao): Nebula should introduce namespace to organize data sources under each user's name/id
  // public data source will be under the default namespace, probably called [PUBLIC]
  // use passed in name as data source name, API loaded table needs to be started with #
  name = fmt::format("{0}{1}", BlockSignature::EPHEMERAL_TABLE_PREFIX, req->table());

  // TTL in seconds
  const auto ttl = req->ttl();

  // google sheet spec in json format.
  const auto& json = req->json();
  // by comparing the json value in table settings
  rapidjson::Document doc;
  if (doc.Parse(json.c_str()).HasParseError()) {
    throw NException(fmt::format("Error parsing google sheet json: {0}", json));
  }

  GoogleSheet sheet{ doc };

  // build a table spec
  KafkaSerde serde;
  ColumnProps props;
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
    serde,
    props,
    sheet.timeSpec,
    sheet.accessSpec,
    BucketInfo::empty(),
    sheet.settings);

  // pattern must be present in settings
  TableService::singleton()->enroll(tbSpec->to(), ttl);

  // build ingestion spec for this google sheet
  // use uid (user ID) as its domain
  LOG(INFO) << "Generate a ingest spec for google sheet: " << sheet.url;
  auto spec = std::make_shared<IngestSpec>(
    tbSpec, "0", sheet.url, sheet.uid, sheet.rows, SpecState::NEW, 0);

  // random assign this spec
  // TODO(cao): need a routine to find a suitable node to balance load on nodes
  auto& ci = ClusterInfo::singleton();
  auto& nodes = ci.nodes();
  auto index = Evidence::rand<size_t>(0, nodes.size() - 1)();
  auto itr = nodes.begin();
  while (index-- > 0) ++itr;
  spec->setAffinity(*itr);

  // could be multiple specs generated by single file
  LoadResult specs{ spec };

  // return the collection  - copy elison
  return specs;
}

// auto detect the schema if not provided
LoadResult LoadHandler::loadDemand(const LoadRequest* req, LoadError& err, std::string& name) {
  // get table name - needs to be unique
  name = fmt::format("{0}{1}", BlockSignature::EPHEMERAL_TABLE_PREFIX, req->table());

  // TTL in seconds
  const auto ttl = req->ttl();

  // google sheet spec in json format.
  const auto& json = req->json();
  // by comparing the json value in table settings
  rapidjson::Document doc;
  if (doc.Parse(json.c_str()).HasParseError()) {
    throw NException(fmt::format("Error parsing load spec json: {0}", json));
  }

  LoadSpec demand{ doc };

  // build a table spec
  auto format = DataFormatUtils::from(demand.format);
  if (format == DataFormat::UNKNOWN) {
    LOG(ERROR) << "Data format is unknown: " << demand.format;
    return {};
  }

  KafkaSerde serde;
  ColumnProps props;
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
    serde,
    props,
    demand.timeSpec,
    demand.accessSpec,
    BucketInfo::empty(),
    demand.settings);

  // pattern must be present in settings
  TableService::singleton()->enroll(tbSpec->to(), ttl);

  // build ingestion spec for this google sheet
  // use uid (user ID) as its domain
  LOG(INFO) << "Generate a ingest spec for load command: " << demand.path;
  auto spec = std::make_shared<IngestSpec>(
    tbSpec, "0", demand.path, demand.domain, 0, SpecState::NEW, 0);

  // random assign this spec
  // TODO(cao): need a routine to find a suitable node to balance load on nodes
  auto& ci = ClusterInfo::singleton();
  auto& nodes = ci.nodes();
  auto index = Evidence::rand<size_t>(0, nodes.size() - 1)();
  auto itr = nodes.begin();
  while (index-- > 0) ++itr;
  spec->setAffinity(*itr);

  // return this spec list
  return { spec };
}

} // namespace server
} // namespace service
} // namespace nebula