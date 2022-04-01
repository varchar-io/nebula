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

#include "SpecProvider.h"

#include "common/Evidence.h"
#include "meta/BucketHelper.h"
#include "storage/NFS.h"
#include "storage/kafka/KafkaTopic.h"

DEFINE_uint64(KAFKA_SPEC_ROWS,
              10000,
              "rows per sepc for kafka ingestion"
              "this value is used in spec identifier so do not modify");
DEFINE_uint64(KAFKA_TIMEOUT_MS, 5000, "Timeout of each Kafka API call");

/**
 *
 * Define table meta data service provider for nebula execution runtime.
 *
 */
namespace nebula {
namespace execution {
namespace meta {

using dsu = nebula::meta::DataSourceUtils;
using nebula::common::Evidence;
using nebula::common::MapKV;
using nebula::meta::BucketHelper;
using nebula::meta::DataSource;
using nebula::meta::DataSpec;
using nebula::meta::Macro;
using nebula::meta::PatternMacro;
using nebula::meta::SpecPtr;
using nebula::meta::SpecSplit;
using nebula::meta::SpecSplitPtr;
using nebula::meta::SpecState;
using nebula::meta::TableSpecPtr;
using nebula::meta::TimeSpec;
using nebula::meta::TimeType;
using nebula::storage::FileInfo;
using nebula::storage::kafka::KafkaSegment;
using nebula::storage::kafka::KafkaTopic;

// specified batch size in table config - not kafka specific
constexpr auto S_BATCH = "batch";
// specified kafka partition /offset to consume - kafka specific
constexpr auto S_PARTITION = "k.partition";
constexpr auto S_OFFSET = "k.offset";

// this method is to generate one spec per file
void genSpecs4Files(const TableSpecPtr& table,
                    const std::string& version,
                    const std::vector<FileInfo>& files,
                    std::vector<SpecPtr>& specs,
                    size_t watermark,
                    const MapKV& macros) noexcept {
  std::vector<SpecSplitPtr> splits;
  size_t batchSize = 0;
  std::string domain;
  for (auto itr = files.cbegin(), end = files.cend(); itr != end; ++itr) {
    if (itr->isDir) {
      VLOG(1) << "Skipping directory: " << itr->name;
      continue;
    }
    if (itr->size == 0) {
      VLOG(1) << "Skip an empty file to scan: " << itr->name;
      continue;
    }
    if (domain.empty()) {
      domain = itr->domain;
    }

    // support bucket if existing (using legacy spark convention)
    // if enabled, populate the bucket value based on settings
    BucketHelper::populate(table, macros);
    splits.push_back(std::make_shared<SpecSplit>(itr->name, itr->size, watermark, macros));

    // generate new spec if current spec exceeds optimal block size (estimation)
    batchSize += itr->size;
    if (batchSize >= table->optimalBlockSize) {
      // push batch and start a new one
      specs.push_back(std::make_shared<DataSpec>(table, version, domain, splits, SpecState::NEW));
      splits.clear();
      batchSize = 0;
    }
  }

  // push the final batch in case it didn't fit evenly
  if (!splits.empty()) {
    specs.push_back(std::make_shared<DataSpec>(table, version, domain, splits, SpecState::NEW));
  }
}

// generate specs for swap type of data
// for swap type, we need file system support:
//  1. list files with timestamp
//  2. each file name will be used as identifier and timestamp will distinguish different data
void genSpecs4Swap(const std::string& version,
                   const TableSpecPtr& table,
                   std::vector<SpecPtr>& specs) noexcept {
  if (dsu::isFileSystem(table->source)) {
    // parse location to get protocol, domain/bucket, path
    auto sourceInfo = nebula::storage::parse(table->location);

    // making a s3 fs with given host
    auto fs = nebula::storage::makeFS(dsu::getProtocol(table->source), sourceInfo.host, table->settings);

    // list all objects/files from given paths
    const auto& pathsWithMacros = Macro::enumeratePathsWithMacros(sourceInfo.path, table->macroValues);
    for (const auto& pathWithCombination : pathsWithMacros) {
      auto files = fs->list(pathWithCombination.first);
      genSpecs4Files(table, version, files, specs, 0, pathWithCombination.second);
    }
    return;
  }

  LOG(WARNING) << "only s3 supported for now";
}

// iterative replace pathTemplate with current level of pattern macro
void genPatternSpec(const PatternMacro macro,
                    const std::string& pathTemplate,
                    const size_t maxSeconds,
                    const std::string& version,
                    const TableSpecPtr& table,
                    std::vector<SpecPtr>& specs) {
  // right now
  const auto now = Evidence::now();
  const auto sourceInfo = nebula::storage::parse(table->location);
  auto fs = nebula::storage::makeFS(dsu::getProtocol(table->source), sourceInfo.host, table->settings);

  // moving step based on macro granularity
  const auto step = Macro::seconds(macro);

  // fill in custom macros
  auto pathsWithMacros = Macro::enumeratePathsWithMacros(pathTemplate, table->macroValues);

  for (const auto& pathMacros : pathsWithMacros) {
    // from now going back step by step until exceeding maxSeconds
    size_t count = 0;
    while (count < maxSeconds) {
      const auto watermark = now - count;
      // populate the file paths for given time point
      const auto path = Macro::materialize(macro, pathMacros.first, watermark);
      // list files in the path and generate spec perf file from it
      genSpecs4Files(table, version, fs->list(path), specs, watermark, pathMacros.second);

      count += step;
    }
  }
}

void genSpecs4Roll(const std::string& version,
                   const TableSpecPtr& table,
                   std::vector<SpecPtr>& specs) noexcept {
  if (dsu::isFileSystem(table->source)) {
    // parse location to get protocol, domain/bucket, path
    auto sourceInfo = nebula::storage::parse(table->location);

    // capture time pattern from path
    auto timePt = Macro::extract(sourceInfo.path);

    // TODO(chenqin): don't support other macro other than dt=date/hr=hour/mi=minute/se=second yet.
    genPatternSpec(timePt,
                   sourceInfo.path,
                   table->max_seconds,
                   version,
                   table,
                   specs);
    return;
  }

  LOG(WARNING) << "File system not supported: " << dsu::getProtocol(table->source);
}

void genRocksetSpec(const std::string& version,
                    const TableSpecPtr& table,
                    std::vector<SpecPtr>& specs) noexcept {
  // produce consistent spec for the time slice, use watermark to store the beginning of the time
  // contract: we provide data start_time and end_time as posted data (UTC time)
  const size_t now = Evidence::now();
  const size_t earliest = now - table->max_seconds;
  const size_t currentHour = Evidence::hour(now);
  const size_t interval = table->rocksetSerde.interval;

  // lambda to add a new spec
  auto add = [&specs, &table, &version](auto watermark) {
    std::vector<SpecSplitPtr> splits = { std::make_shared<SpecSplit>(table->location, 0, watermark) };
    specs.push_back(std::make_shared<DataSpec>(table, version, "rockset", splits, SpecState::NEW));
  };

  // going back from hour start
  size_t watermark = currentHour - interval;
  while (watermark > earliest) {
    add(watermark);
    watermark -= interval;
  }

  // going forward from hour start
  watermark = currentHour;
  while (watermark + interval < now) {
    add(watermark);
    watermark += interval;
  }
}

void genKafkaSpec(const std::string& version,
                  const TableSpecPtr& table,
                  std::vector<SpecPtr>& specs) noexcept {
  // visit each partition of the topic and figure out range for each spec
  // stream is different as static file, to make it reproducible, we need
  // a stable spec generation based on offsets, every N (eg. 10K) records per spec
  KafkaTopic topic(table->location, table->kafkaSerde, table->settings, FLAGS_KAFKA_TIMEOUT_MS);

  // check if this table has set batch size to overwrite the default one
  const auto& settings = table->settings;
  auto batch = FLAGS_KAFKA_SPEC_ROWS;
  auto itr = settings.find(S_BATCH);
  if (itr != settings.end()) {
    batch = folly::to<size_t>(itr->second);
    VLOG(1) << "Table " << table->name << " overwrite batch size as " << batch;
  }

  // turn these segments into ingestion spec
  auto convert = [&specs, &table, &version](const std::list<KafkaSegment>& segments) {
    // turn these segments into ingestion spec
    for (auto itr = segments.cbegin(), end = segments.cend(); itr != end; ++itr) {
      std::vector<SpecSplitPtr> splits = { std::make_shared<SpecSplit>(itr->id(), itr->size, 0) };
      specs.push_back(std::make_shared<DataSpec>(table, version, "kafka", splits, SpecState::NEW));
    }
  };

  // if specific partition / offset specified, we only consume it.
  // this is usually for debugging purpose
  auto itr_p = settings.find(S_PARTITION);
  auto itr_o = settings.find(S_OFFSET);
  if (itr_p != settings.end() && itr_o != settings.end()) {
    std::list<KafkaSegment> segments;
    segments.emplace_back(folly::to<int32_t>(itr_p->second), folly::to<int64_t>(itr_o->second), batch);
    convert(segments);
    return;
  }

  // set start time
  const auto startMs = 1000 * (Evidence::unix_timestamp() - table->max_seconds);
  auto segments = topic.segmentsByTimestamp(startMs, batch);
  convert(segments);
}

std::string buildIdentityByTime(const TimeSpec& time) {
  switch (time.type) {
  case TimeType::STATIC: {
    // the static time stamp value is its identity
    return folly::to<std::string>(time.unixTimeValue);
  }
  case TimeType::CURRENT: {
    return folly::to<std::string>(Evidence::unix_timestamp());
  }
  default: return "";
  }
}

// given table specification - generate specs
std::vector<SpecPtr> SpecProvider::generate(
  const std::string& version, const nebula::meta::TableSpecPtr& table) noexcept {
  std::vector<SpecPtr> specs;
  // specialized loader handling - nebula test set identified by static time provided
  if (table->loader == "NebulaTest") {
    // single spec for nebula test loader
    std::vector<SpecSplitPtr> splits = { std::make_shared<SpecSplit>(buildIdentityByTime(table->timeSpec), 0, 0) };
    specs.push_back(std::make_shared<DataSpec>(table, version, table->name, splits, SpecState::NEW));
    return specs;
  }

  // S3/GCS has two mode:
  // 1. swap data when renewed or
  // 2. roll data clustered by time
  if (dsu::isFileSystem(table->source)) {
    if (table->loader == "Swap") {
      genSpecs4Swap(version, table, specs);
      return specs;
    }

    if (table->loader == "Roll") {
      genSpecs4Roll(version, table, specs);
      return specs;
    }
  }

  if (table->source == DataSource::KAFKA) {
    genKafkaSpec(version, table, specs);
    return specs;
  }

  // generate time slice api to load data from rockset api
  if (table->source == DataSource::ROCKSET) {
    genRocksetSpec(version, table, specs);
    return specs;
  }

  // API are data load request from API layer
  if (table->loader == "Api") {
    // TODO(cao): refactor macro enumeration in multiple places
    // since the location is probably URL encoded, hence {X} could be found as "%7BX%7D"
    // but we can't decode the whole uri since the url might be partially encoded
    const auto& mv = table->macroValues;
    std::vector<std::string> macroNames;
    for (auto& kv : mv) {
      macroNames.push_back(kv.first);
    }
    const auto pathTemplate = Macro::restoreTemplate(table->location, macroNames);
    auto pathsWithMacros = Macro::enumeratePathsWithMacros(pathTemplate, mv);
    for (const auto& pathMacros : pathsWithMacros) {
      std::vector<SpecSplitPtr> splits = { std::make_shared<SpecSplit>(pathMacros.first, 0, 0, pathMacros.second) };
      specs.push_back(std::make_shared<DataSpec>(table, version, "api", splits, SpecState::NEW));
    }

    return specs;
  }

  // template is used for API to materialize it - so no warning
  if (table->loader != "Template") {
    LOG(WARNING) << fmt::format("Unsupported loader: {0} for table {1}",
                                table->loader, table->toString());
  }

  return specs;
}

} // namespace meta
} // namespace execution
} // namespace nebula