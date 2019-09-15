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

constexpr auto DAY_SECONDS = 3600 * 24;

// generate a list of ingestion spec based on cluster info
void SpecRepo::refresh(const ClusterInfo& ci) {
  // we only support adding new spec to the repo
  // if a spec is already in repo, we skip it
  // for some use case such as data refresh, it will have the same signature
  // if data is newer (e.g file size + timestamp), we should mark it as replacement.
  std::vector<std::shared_ptr<IngestSpec>> specs;
  const auto& tableSpecs = ci.tables();

  // generate a version all spec to be made during this batch: {config version}_{current unix timestamp}
  const auto version = fmt::format("{0}.{1}", ci.version(), nebula::common::Evidence::unix_timestamp());
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
    return folly::to<std::string>(nebula::common::Evidence::unix_timestamp());
  }
  default: return "";
  }
}

// this method is to generate one spec per file
void genSpecPerFile(const TableSpecPtr& table,
                    const std::string& version,
                    const std::vector<FileInfo>& files,
                    std::vector<std::shared_ptr<IngestSpec>>& specs,
                    size_t macroDate) {
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
                   std::vector<std::shared_ptr<IngestSpec>>& specs) {
  if (table->source == DataSource::S3) {
    // parse location to get protocol, domain/bucket, path
    auto sourceInfo = nebula::storage::parse(table->location);

    // making a s3 fs with given host
    auto fs = nebula::storage::makeFS("s3", sourceInfo.host);

    // list all objects/files from given path
    auto files = fs->list(sourceInfo.path);
    LOG(INFO) << fmt::format("list {0}:{1} = {2}", sourceInfo.host, sourceInfo.path, files.size());
    genSpecPerFile(table, version, files, specs, 0);
    return;
  }

  throw NException("only s3 supported for now");
}

void genSpecs4Roll(const std::string& version,
                   const TableSpecPtr& table,
                   std::vector<std::shared_ptr<IngestSpec>>& specs) {
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

  throw NException("only s3 supported for now");
}

void SpecRepo::process(
  const std::string& version,
  const TableSpecPtr& table,
  std::vector<std::shared_ptr<IngestSpec>>& specs) {
  // specialized loader handling - nebula test set identified by static time provided
  if (table->loader == "NebulaTest") {
    // single spec for nebula test loader
    specs.push_back(std::make_shared<IngestSpec>(
      table, version, buildIndentityByTime(table->timeSpec), "nebula.test", 0, SpecState::NEW, 0));
    return;
  }

  if (table->loader == "Swap") {
    genSpecs4Swap(version, table, specs);
    return;
  }

  if (table->loader == "Roll") {
    genSpecs4Roll(version, table, specs);
    return;
  }

  throw NException(fmt::format("Unsupported loader: {0} for table {1}", table->loader, table->toString()));
}

void SpecRepo::update(const std::vector<std::shared_ptr<IngestSpec>>& specs) {
  // go through the new spec list and update the existing ones
  // need lock here?
  for (auto itr = specs.cbegin(), end = specs.cend(); itr != end; ++itr) {
    // check if we have this spec already?
    auto specPtr = (*itr);
    const auto& id = specPtr->id();
    auto found = specs_.find(id);
    if (found == specs_.end()) {
      specs_.emplace(id, specPtr);
    } else {
      // TODO(cao) - use only size for the checker for now, may extend to other properties
      // this is an update case, otherwise, spec doesn't change, ignore it.
      if (specPtr->size() != found->second->size()) {
        found->second->setState(SpecState::RENEW);
      }
    }
  }
}

void SpecRepo::assign(const ClusterInfo& ci) {
  // assign each spec to a node if it needs to be processed
  // TODO(cao) - build resource constaints here to reach a balance
  // for now, we just round robin to spin into each slot
  const NNodeSet& ns = ci.nodes();
  const std::vector<NNode> nodes(ns.cbegin(), ns.cend());
  const auto size = nodes.size();

  N_ENSURE_GT(size, 0, "No nodes to assign nebula spec");
  size_t idx = 0;

  // for each spec
  for (auto& spec : specs_) {
    // not assigned yet
    auto sp = spec.second;
    if (sp->affinity().isInvalid()) {
      sp->setAffinity(nodes.at(idx));
      idx = (idx + 1) % size;
    }
  }
}

} // namespace ingest
} // namespace nebula