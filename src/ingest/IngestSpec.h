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

#include <fmt/format.h>
#include <mutex>

#include "meta/NNode.h"

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

// spec state defines states for life cycle of given spec
enum class SpecState : char {
  // NEW spec requires data sync
  NEW = 'N',

  // data of the spec loaded in nebula
  READY = 'A',

  // Spec is updated, data needs renew
  RENEW = 'R',

  // Spec is waiting for offload
  EXPIRED = 'E'
};

// a ingest spec defines a task specification to ingest some data
class IngestSpec {
public:
  IngestSpec(const std::string& version, const std::string& id)
    : version_{ version },
      id_{ id },
      size_{ 0 },
      state_{ SpecState::NEW },
      node_{ nebula::meta::NNode::invalid() } {}
  virtual ~IngestSpec() = default;

  inline std::string toString() const {
    return fmt::format("[IS {0} - {1}]", version_, id_);
  }

  inline void setSize(size_t size) {
    size_ = size;
  }

  inline void setState(SpecState state) {
    state_ = state;
  }

  inline void setAffinity(const nebula::meta::NNode& node) {
    // copy the node as my node
    node_ = node;
  }

  inline const std::string& id() const {
    return id_;
  }

  inline size_t size() const {
    return size_;
  }

  inline const nebula::meta::NNode& affinity() const {
    return node_;
  }

  inline bool assigned() const {
    return !node_.isInvalid();
  }

  inline bool needSync() const {
    return state_ != SpecState::READY;
  }

  inline SpecState state() const {
    return state_;
  }

private:
  std::string version_;
  std::string id_;
  size_t size_;
  SpecState state_;

  // node info if the spec has affinity on a node
  nebula::meta::NNode node_;
};

} // namespace ingest
} // namespace nebula