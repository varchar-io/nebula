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

#include "NNode.h"
#include "Table.h"
#include "common/Likely.h"

/**
 * Define nebula cell - a data block or segment metadaeta that loaded in memory.
 */
namespace nebula {
namespace meta {

struct BlockState {
  size_t numRows;
  size_t rawSize;
};

struct BlockSignature {
  explicit BlockSignature(const std::string& t, size_t i, size_t s, size_t e, const std::string& sp = "")
    : table{ t }, id{ i }, start{ s }, end{ e }, spec{ sp } {}
  // table name
  std::string table;
  size_t id;

  // start/end timestamp
  size_t start;
  size_t end;
  std::string spec;

  inline bool sameSpec(const BlockSignature& another) const {
    // same object by comparing address
    if (UNLIKELY(std::addressof(another) == std::addressof(*this))) {
      return true;
    }

    // same table and same spec -
    // ?? do we have case where different table sharing the same spec
    return spec == another.spec
           && table == another.table;
  }

  inline std::string toString() const {
    return fmt::format("{0}_{1}_{2}_{3}_{4}", table, spec, id, start, end);
  }

  friend bool operator==(const BlockSignature& x, const BlockSignature& y) {
    return x.toString() == y.toString();
  }
};

template <typename T>
class NBlock {
public:
  // define a nblock that has a unique ID and start and end time stamp
  NBlock(const BlockSignature& sign, const NNode& node, const BlockState& state)
    : NBlock(sign, node, nullptr, state) {}

  // define a nblock that has a unique ID and start and end time stamp
  NBlock(const BlockSignature& sign, std::shared_ptr<T> data, const BlockState& state)
    : NBlock(sign, NNode::inproc(), data, state) {}

  virtual ~NBlock() = default;

  friend bool operator==(const NBlock& x, const NBlock& y) {
    return x.sign_ == y.sign_;
  }

  inline const std::string& getTable() const {
    return sign_.table;
  }

  inline size_t getId() const {
    return sign_.id;
  }

  inline const BlockSignature& signature() const {
    return sign_;
  }

  inline const std::string& spec() const {
    return sign_.spec;
  }

  inline const BlockState& state() const {
    return state_;
  }

  inline const NNode& residence() const {
    return residence_;
  }

  inline const std::shared_ptr<T>& data() const {
    return data_;
  }

  inline bool inRange(size_t time) const {
    return time >= sign_.start && time <= sign_.end;
  }

  inline bool overlap(const std::pair<size_t, size_t>& window) const noexcept {
    if ((window.second < sign_.start) || (sign_.end < window.first)) {
      return false;
    }

    return true;
  }

  inline size_t start() const {
    return sign_.start;
  }

  inline size_t end() const {
    return sign_.end;
  }

  size_t hash() const {
    return hash_;
  }

  inline const std::string& storage() const {
    return storage_;
  }

private:
  NBlock(const BlockSignature& sign, const NNode& node, std::shared_ptr<T> data, const BlockState& state)
    : sign_{ sign }, data_{ data }, residence_{ std::move(node) }, state_{ state } {
    hash_ = hash(*this);
  }

  // An hash algo example
  // auto h2 = b.id_;
  // size_t k = 0xC6A4A7935BD1E995UL;
  // h2 = ((h2 * k) >> 47) * k;
  // return (h1 ^ h2) * k;
  static size_t hash(const NBlock& b) {
    auto strSign = b.signature().toString();
    return std::hash<std::string>()(strSign);
  }

private:
  BlockSignature sign_;

  // data pointer to memory block if resident inproc.
  // otherwise nullptr
  std::shared_ptr<T> data_;

  // residence node to locate this block
  NNode residence_;

  // metrics of current block: number of rows, data raw size
  BlockState state_;

  // a signature of current block - use 64bit GUID?
  size_t hash_;

  // a uniuqe identifier to find this block data in storage.
  std::string storage_;
};

} // namespace meta
} // namespace nebula