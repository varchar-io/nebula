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

#pragma once

#include <meta/TableSpec.h>

#include "IngestSpec.h"
#include "common/Hash.h"
#include "meta/ClusterInfo.h"
#include "meta/Macro.h"
#include "meta/TableSpec.h"

/**
 * Based on current cluster settings. 
 * Generate ingestion spec list for each table.
 * 
 * A spec repo is held by a nebula serve which holds the truth of all data knowledge.
 * We usually run a SpecRepo gen methods periodically.
 * 
 * Every spec will be check against current data store to indicate if a spec needs to be ingested.
 * Server will ask a node to ingest the spec. So spec state usually transits like
 *    "new" -> "assigned" -> "done"
 * 
 * A spec may produce multiple data blocks, if anything failing in the middle, 
 * the data node will clean them up and tell server it doesn't finish the task. Server will re-assign later.
 */
namespace nebula {
namespace ingest {

class SpecRepo {
  using SpecPtr = std::shared_ptr<IngestSpec>;

public:
  // parse pattern string and generate ingest spec
  static void genPatternSpec(
    const nebula::meta::PatternMacro, const std::string&,
    const size_t, const std::string&, const nebula::meta::TableSpecPtr&,
    std::vector<std::shared_ptr<IngestSpec>>&);

public:
  SpecRepo() = default;
  virtual ~SpecRepo() = default;

  // refresh spec repo based on cluster configs
  void refresh(const nebula::meta::ClusterInfo&) noexcept;

  // this method can be sub-routine of refresh
  void assign(const std::vector<nebula::meta::NNode>&) noexcept;

  // expose all current specs in repo
  inline const auto& specs() const {
    return specs_;
  }

  // try to assign a node to a spec
  // assign the spec for given node
  bool confirm(const std::string& spec, const nebula::meta::NNode& node) noexcept;

private:
  // process a table spec and generate all specs into the given specs container
  void process(const std::string&, const nebula::meta::TableSpecPtr&, std::vector<SpecPtr>&) noexcept;

  // update the snapshot of new spec list into spec repo
  void update(const std::vector<SpecPtr>&) noexcept;

  void genSpecs4Roll(const std::string& version,
                     const meta::TableSpecPtr& table,
                     std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept;

private:
  nebula::common::unordered_map<std::string, SpecPtr> specs_;
};

} // namespace ingest
} // namespace nebula