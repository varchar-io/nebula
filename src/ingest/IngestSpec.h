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

#include <fmt/format.h>
#include <forward_list>
#include <mutex>

#include "common/Task.h"
#include "execution/io/BlockLoader.h"
#include "meta/DataSpec.h"
#include "meta/NNode.h"
#include "meta/TableSpec.h"

/**
 * A ingest spec is generated from table setting based on its ingestion type.
 * To ensure system not act on the same data source, every spec should be identifiable.
 *
 * Such as swap table can be identified by file name + file modify time.
 * Roll table needs to provide rolling date as its identfier.
 *
 * In the long run, every single data source needs to be identified by a version.
 * A version should be a source of truth to check if a data is different or not.
 *
 * This principle should apply in nebula as well.
 */
namespace nebula {
namespace ingest {

// a ingest spec defines a task specification to ingest some data
class IngestSpec : public nebula::meta::DataSpec {
public:
  IngestSpec(nebula::meta::TableSpecPtr table,
             const std::string& version,
             const std::string& domain,
             const std::vector<nebula::meta::SpecSplitPtr>& splits,
             nebula::meta::SpecState state)
    : DataSpec(table, version, domain, splits, state),
      node_{ nebula::meta::NNode::invalid() } {}
  virtual ~IngestSpec() = default;

  inline void setAffinity(const nebula::meta::NNode& node) {
    // copy the node as my node
    node_ = node;
  }

  inline const nebula::meta::NNode& affinity() const {
    return node_;
  }

  inline bool assigned() const {
    return !node_.isInvalid();
  }

  // core work to process this ingest spec
  bool work() noexcept;

private:
  // load swap
  bool loadSwap() noexcept;

  // load roll
  bool loadRoll() noexcept;

  // load api - on demand ingestion
  bool loadApi() noexcept;

  // load kafka
  bool loadKafka(nebula::meta::SpecSplitPtr) noexcept;

  // load rockset data
  bool loadRockset(nebula::meta::SpecSplitPtr) noexcept;

  // load google sheet
  std::unique_ptr<nebula::surface::RowCursor> readGSheet() noexcept;

  // load an http resource
  bool loadHttp(nebula::execution::io::BlockList&,
                nebula::meta::SpecSplitPtr,
                std::vector<std::string> = {},
                std::string_view = "") noexcept;

  // load current spec as blocks
  bool load(nebula::execution::io::BlockList&) noexcept;

  // ingest will expect all files are downloaded
  bool ingest(nebula::execution::io::BlockList&) noexcept;

private:
  // node info if the spec has affinity on a node
  nebula::meta::NNode node_;
};

} // namespace ingest
} // namespace nebula