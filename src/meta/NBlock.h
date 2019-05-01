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

#include <glog/logging.h>
#include "Table.h"

/**
 * Define nebula cell - a data block or segment metadaeta that loaded in memory.
 */
namespace nebula {
namespace meta {

class NBlock {
public:
  // define a nblock that has a unique ID and start and end time stamp
  NBlock(const std::string& tbl, size_t blockId, size_t start, size_t end)
    : table_{ tbl }, id_{ blockId }, start_{ start }, end_{ end } {
    hash_ = hash(*this);
    sign_ = fmt::format("{0}_{1}_{2}_{3}", table_, id_, start_, end_);
  }

  virtual ~NBlock() = default;

  friend bool operator==(const NBlock& x, const NBlock& y) {
    return x.table_ == y.table_ && x.id_ == y.id_;
  }

  inline const std::string& getTable() const {
    return table_;
  }

  inline size_t getId() const {
    return id_;
  }

  inline std::string signature() const {
    return sign_;
  }

  inline bool inRange(size_t time) const {
    return time >= start_ && time <= end_;
  }

  inline bool overlap(const std::pair<size_t, size_t>& window) const noexcept {
    if ((window.second < start_) || (end_ < window.first)) {
      return false;
    }

    return true;
  }

  inline size_t start() const {
    return start_;
  }

  inline size_t end() const {
    return end_;
  }

  size_t hash() const {
    return hash_;
  }

  static size_t hash(const NBlock& b) {
    LOG(INFO) << "compute hash of nblock: " << b.signature();
    auto h1 = std::hash<std::string>()(b.table_);
    auto h2 = b.id_;
    size_t k = 0xC6A4A7935BD1E995UL;
    h2 = ((h2 * k) >> 47) * k;
    return (h1 ^ h2) * k;
  }

private:
  std::string table_;
  // sequence ID
  size_t id_;

  // this should be time_t, it defines time range of data for this block
  size_t start_;
  size_t end_;

  // a signature of current block - use 64bit GUID?
  size_t hash_;
  std::string sign_;

  // a uniuqe identifier to find this block data in storage.
  std::string storage_;
};

} // namespace meta
} // namespace nebula