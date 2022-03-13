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

#pragma once

#include "IngestSpec.h"

#include "common/Hash.h"
#include "execution/core/NodeConnector.h"
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
public:
  SpecRepo() = default;
  virtual ~SpecRepo() = default;

  // refresh spec repo based on cluster configs
  size_t refresh() noexcept;

  // remove all expired blocks from active nodes
  std::vector<nebula::meta::NNode> expire(
    std::function<std::unique_ptr<nebula::execution::core::NodeClient>(const nebula::meta::NNode&)>) noexcept;

  // this method can be sub-routine of refresh
  void assign(const std::vector<nebula::meta::NNode>&) noexcept;
};

} // namespace ingest
} // namespace nebula