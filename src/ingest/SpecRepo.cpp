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

#include <fmt/format.h>
#include <folly/Conv.h>

#include "SpecRepo.h"
#include "common/Evidence.h"
#include "storage/NFS.h"
#include "storage/kafka/KafkaTopic.h"

DEFINE_uint64(KAFKA_SPEC_ROWS,
              10000,
              "rows per sepc for kafka ingestion"
              "this value is used in spec identifier so do not modify");
DEFINE_uint64(KAFKA_TIMEOUT_MS, 5000, "Timeout of each Kafka API call");

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace ingest {

using nebula::common::Evidence;
using nebula::meta::ClusterInfo;
using nebula::meta::DataSource;
using nebula::meta::NNode;
using nebula::meta::NNodeSet;
using nebula::meta::TableSpecPtr;
using nebula::meta::TimeSpec;
using nebula::meta::TimeType;
using nebula::storage::FileInfo;
using nebula::storage::kafka::KafkaTopic;

constexpr auto HOUR_SECONDS = 3600;
constexpr auto DAY_SECONDS = HOUR_SECONDS * 24;

// generate a list of ingestion spec based on cluster info
void SpecRepo::refresh(const ClusterInfo& ci) noexcept {
  // we only support adding new spec to the repo
  // if a spec is already in repo, we skip it
  // for some use case such as data refresh, it will have the same signature
  // if data is newer (e.g file size + timestamp), we should mark it as replacement.
  std::vector<std::shared_ptr<IngestSpec>> specs;
  const auto& tableSpecs = ci.tables();

  // generate a version all spec to be made during this batch: {config version}_{current unix timestamp}
  const auto version = fmt::format("{0}.{1}", ci.version(), Evidence::unix_timestamp());
  for (auto itr = tableSpecs.cbegin(); itr != tableSpecs.cend(); ++itr) {
    process(version, *itr, specs);
  }

  // process all specs to mark their status
  update(specs);
}

std::string buildIndentityByTime(const TimeSpec& time) {
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

// this method is to generate one spec per file
void genSpecPerFile(const TableSpecPtr& table,
                    const std::string& version,
                    const std::vector<FileInfo>& files,
                    std::vector<std::shared_ptr<IngestSpec>>& specs,
                    size_t macroDate) noexcept {
  for (auto itr = files.cbegin(), end = files.cend(); itr != end; ++itr) {
    if (!itr->isDir) {
      // generate a ingest spec from given file info
      // use name as its identifier
      auto spec = std::make_shared<IngestSpec>(
        table, version, itr->name, itr->domain, itr->size, SpecState::NEW, macroDate);

      // push to the repo
      specs.push_back(spec);
    }
  }
}

// generate specs for swap type of data
// for swap type, we need file system support:
//  1. list files with timestamp
//  2. each file name will be used as identifier and timestamp will distinguish different data
void genSpecs4Swap(const std::string& version,
                   const TableSpecPtr& table,
                   std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  if (table->source == DataSource::S3) {
    // parse location to get protocol, domain/bucket, path
    auto sourceInfo = nebula::storage::parse(table->location);

    // making a s3 fs with given host
    auto fs = nebula::storage::makeFS("s3", sourceInfo.host);

    // list all objects/files from given path
    auto files = fs->list(sourceInfo.path);
    genSpecPerFile(table, version, files, specs, 0);
    return;
  }

  LOG(WARNING) << "only s3 supported for now";
}

void genSpecs4Roll(const std::string& version,
                   const TableSpecPtr& table,
                   std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  if (table->source == DataSource::S3) {
    // parse location to get protocol, domain/bucket, path
    auto sourceInfo = nebula::storage::parse(table->location);

    // making a s3 fs with given host
    auto fs = nebula::storage::makeFS("s3", sourceInfo.host);

    // list all objects/files from given path
    // A roll spec will cover X days given table location of source data
    const auto now = Evidence::now();
    const auto max_days = table->max_hr / 24;
    for (size_t i = 0; i <= max_days; ++i) {
      // we only provide single macro for now
      auto timeValue = now - i * DAY_SECONDS;
      auto path = fmt::format(
        sourceInfo.path, fmt::arg("date", Evidence::fmt_ymd_dash(timeValue)));
      auto files = fs->list(path);
      genSpecPerFile(table, version, files, specs, timeValue);
    }

    return;
  }

  LOG(WARNING) << "only s3 supported for now";
}

void genKafkaSpec(const std::string& version,
                  const TableSpecPtr& table,
                  std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  // visit each partition of the topic and figure out range for each spec
  // stream is different as static file, to make it reproducible, we need
  // a stable spec generation based on offsets, every N (eg. 10K) records per spec
  KafkaTopic topic(table->location, table->name, table->serde, FLAGS_KAFKA_TIMEOUT_MS);

  // set start time
  const auto startMs = 1000 * (Evidence::unix_timestamp() - table->max_hr * HOUR_SECONDS);
  auto segments = topic.segmentsByTimestamp(startMs, FLAGS_KAFKA_SPEC_ROWS);

  // turn these segments into ingestion spec
  for (auto itr = segments.cbegin(), end = segments.cend(); itr != end; ++itr) {
    specs.push_back(std::make_shared<IngestSpec>(
      table, version, itr->id(), "kafka", itr->size, SpecState::NEW, 0));
  }
}

void SpecRepo::process(
  const std::string& version,
  const TableSpecPtr& table,
  std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  // specialized loader handling - nebula test set identified by static time provided
  if (table->loader == "NebulaTest") {
    // single spec for nebula test loader
    specs.push_back(std::make_shared<IngestSpec>(
      table, version, buildIndentityByTime(table->timeSpec), "nebula.test", 0, SpecState::NEW, 0));
    return;
  }

  // S3 has two mode:
  // 1. swap data when renewed or
  // 2. roll data clustered by time
  if (table->source == DataSource::S3) {
    if (table->loader == "Swap") {
      genSpecs4Swap(version, table, specs);
      return;
    }

    if (table->loader == "Roll") {
      genSpecs4Roll(version, table, specs);
      return;
    }

    if (table->loader == "Api") {
      // We're not loading data for API
      return;
    }
  }

  if (table->source == DataSource::KAFKA) {
    genKafkaSpec(version, table, specs);
    return;
  }

  LOG(WARNING) << fmt::format("Unsupported loader: {0} for table {1}",
                              table->loader, table->toString());
}

void SpecRepo::update(const std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  // next version of all specs
  std::unordered_map<std::string, SpecPtr> next;
  next.reserve(specs.size());

  // go through the new spec list and update the existing ones
  // need lock here?
  auto brandnew = 0;
  auto renew = 0;
  auto removed = specs_.size() - specs.size();
  for (auto itr = specs.cbegin(), end = specs.cend(); itr != end; ++itr) {
    // check if we have this spec already?
    auto specPtr = (*itr);
    const auto& sign = specPtr->signature();
    auto found = specs_.find(sign);
    if (found == specs_.end()) {
      ++brandnew;
    } else {
      auto prev = found->second;

      // by default, we carry over existing spec's properties
      const auto& node = prev->affinity();
      specPtr->setAffinity(node);
      specPtr->setState(prev->state());

      // TODO(cao) - use only size for the checker for now, may extend to other properties
      // this is an update case, otherwise, spec doesn't change, ignore it.
      if (specPtr->size() != prev->size()) {
        specPtr->setState(SpecState::RENEW);
        ++renew;
      }

      // if the node is not active, we may remove the affinity to allow new assignment
      if (!node.isActive()) {
        specPtr->setAffinity(NNode::invalid());
      }
    }

    // move to the next version
    next.emplace(sign, specPtr);
  }

  // print out update stats
  if (brandnew > 0 || renew > 0 || removed > 0) {
    LOG(INFO) << "Updating " << specs.size()
              << " specs: brandnew=" << brandnew
              << ", renew=" << renew
              << ", removed=" << removed
              << ", count=" << next.size();

    // let's swap with existing one
    if (specs.size() != next.size()) {
      LOG(WARNING) << "No duplicate specs allowed.";
    }

    std::swap(specs_, next);
  }
}

bool SpecRepo::assign(const std::string& spec, const nebula::meta::NNode& node) noexcept {
  auto f = specs_.find(spec);
  // not found
  if (f == specs_.end()) {
    return false;
  }

  // reuse the same node for the same spec
  auto& sp = f->second;
  if (!sp->assigned()) {
    sp->setAffinity(node);
    return false;
  }

  // not in the same node
  auto& assignment = sp->affinity();
  if (!assignment.equals(node)) {
    LOG(INFO) << "Spec [" << spec << "] moves from " << node.server << " to " << assignment.server;
    return false;
  }

  return true;
}

void SpecRepo::assign(const std::vector<NNode>& nodes) noexcept {
  // we're looking for a stable assignmet, given the same set of nodes
  // this order is most likely having stable order
  // std::sort(nodes.begin(), nodes.end(), [](auto& n1, auto& n2) {
  //   return n1.server.compare(n2.server);
  // });
  const auto size = nodes.size();

  if (size == 0) {
    LOG(WARNING) << "No nodes to assign nebula specs.";
    return;
  }

  size_t idx = 0;

  // for each spec
  // TODO(cao): should we do hash-based shuffling here to ensure a stable assignment?
  // Round-robin is easy to break the position affinity whenever new spec is coming
  // Or we can keep order of the specs so that any old spec is associated.
  for (auto& spec : specs_) {
    // not assigned yet
    auto sp = spec.second;
    if (!sp->assigned()) {
      auto startId = idx;
      while (true) {
        auto& n = nodes.at(idx);
        if (n.isActive()) {
          sp->setAffinity(n);
          idx = (idx + 1) % size;
          break;
        }

        idx = (idx + 1) % size;
        if (idx == startId) {
          LOG(ERROR) << "No active node found to assign spec.";
          return;
        }
      }
    }
  }
}

} // namespace ingest
} // namespace nebula