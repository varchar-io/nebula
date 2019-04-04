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

#include "Table.h"
#include "glog/logging.h"

/**
 * Define nebula cell - a data block or segment metadaeta that loaded in memory.
 */
namespace nebula {
namespace meta {

class NBlock {
public:
  NBlock(const Table& tbl, size_t blockId) : table_{ tbl }, id_{ blockId } {
    auto h = hash();
    sign_ = fmt::format("{0}_{1}_{2}", table_.name(), id_, h);
  }

  virtual ~NBlock() = default;

  friend bool operator==(const NBlock& x, const NBlock& y) {
    return x.table_.name() == y.table_.name() && x.id_ == y.id_;
  }

  inline const Table& getTable() const {
    return table_;
  }

  inline size_t getId() const {
    return id_;
  }

  inline std::string signature() const {
    return sign_;
  }

  size_t hash() const {
    LOG(INFO) << "compute hash of nblock...";
    auto h1 = std::hash<std::string>()(table_.name());
    auto h2 = id_;
    size_t k = 0xC6A4A7935BD1E995UL;
    h2 = ((h2 * k) >> 47) * k;
    return (h1 ^ h2) * k;
  }

private:
  // guid?
  size_t id_;
  // a uniuqe identifier to find this block data in storage.
  std::string storage_;
  Table table_;
  std::string sign_;
};

} // namespace meta
} // namespace nebula