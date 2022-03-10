
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
#include <glog/logging.h>
#include <sstream>

#include "common/Identifiable.h"
#include "meta/TableSpec.h"
#include "meta/Types.h"

/**
 * A generic data spec interface that loads one or more data blocks in runtime.
 */

namespace nebula {
namespace meta {

// one spec may have multiple files if backed by file storage
struct SpecSplit : public nebula::common::Identifiable {
  // default constructor for serde
  SpecSplit() {}
  SpecSplit(std::string p, size_t o, size_t s, size_t w, MapKV m)
    : path{ std::move(p) },
      offset{ o },
      size{ s },
      watermark{ w },
      macros{ std::move(m) } {
    construct();
  }
  // path, size, watermark and macros - 0 offset
  SpecSplit(std::string p, size_t s, size_t w, MapKV m)
    : SpecSplit(std::move(p), 0, s, w, std::move(m)) {}
  // path, size and watermark - 0 offset, empty macros
  SpecSplit(std::string p, size_t s, size_t w)
    : SpecSplit(std::move(p), 0, s, w, MapKV{}) {}
  virtual ~SpecSplit() = default;

  // split path
  std::string path;

  // offset of the path for this split - default to 0
  size_t offset;

  // size of the split
  size_t size;

  // watermark of the split
  size_t watermark;

  // if the split is generated from template, it will have key-value macros
  MapKV macros;

  // a local disk file that holds data copy of current split for temporary
  std::string local;

  // build identifier of this split
  inline virtual const std::string& id() const override {
    return id_;
  }

  inline virtual void construct() override {
    id_ = fmt::format("{0}#{1}#{2}#{3}", path, offset, size, watermark);
  }

  // serialization of this object
  MSGPACK_DEFINE(path, offset, size, watermark, macros);

private:
  std::string id_;
};

// data spec: defines a logical data unit which could produce multiple data blocks
using SpecSplitPtr = std::shared_ptr<SpecSplit>;
class DataSpec;
using SpecPtr = std::shared_ptr<DataSpec>;

// definition of data spec
class DataSpec : public nebula::common::Identifiable {
public:
  // default constructor for serde
  DataSpec() {}
  DataSpec(nebula::meta::TableSpecPtr table,
           const std::string& version,
           const std::string& domain,
           const std::vector<SpecSplitPtr>& splits,
           SpecState state)
    : table_{ table },
      version_{ version },
      domain_{ domain },
      splits_{ splits },
      state_{ state } {
    construct();
  }

  virtual ~DataSpec() = default;

public:
  inline virtual const std::string& id() const override {
    return id_;
  }

  inline virtual void construct() override {
    N_ENSURE(splits_.size() > 0, "Requires at least one split for a spec.");
    // computed value for reuse
    id_ = genId();
    size_ = std::accumulate(splits_.begin(), splits_.end(), 0, [](size_t x, const auto& s) {
      return x + s->size;
    });
  }

  inline void state(SpecState state) {
    state_ = state;
  }

  inline SpecState state() const {
    return state_;
  }

  inline const std::string& version() const {
    return version_;
  }

  inline size_t size() const {
    return size_;
  }

  inline nebula::meta::TableSpecPtr table() const {
    return table_;
  }

  inline const std::string& domain() const {
    return domain_;
  }

  inline const std::vector<SpecSplitPtr>& splits() const {
    return splits_;
  }

  // a spec needs to sync to a node when it's NEW or RENEW
  inline bool needSync() const {
    return (state_ == SpecState::NEW || state_ == SpecState::RENEW);
  }

protected:
  // get the first and the only split for most single split use cases
  inline SpecSplitPtr split() const {
    if (splits_.size() != 1) {
      LOG(WARNING) << "Current spec has multiple splits: " << splits_.size();
    }

    return splits_.at(0);
  }

private:
  // id: "<table>@[split,...]"
  inline std::string genId() const noexcept {
    std::stringstream ss;
    ss << table_->name << "@[";
    for (auto& split : splits_) {
      ss << split->id() << ",";
    }
    ss << "]";
    return ss.str();
  }

protected:
  nebula::meta::TableSpecPtr table_;
  std::string version_;
  // could be s3 bucket or other protocol
  std::string domain_;
  std::vector<SpecSplitPtr> splits_;
  SpecState state_;

  // computed identifier during construction
  size_t size_;
  std::string id_;

public:
  MSGPACK_DEFINE(table_, version_, domain_, splits_, state_);
  // serialize a table spec into a string
  static std::string serialize(const DataSpec&) noexcept;
  // deserialize a table spec from a string
  static SpecPtr deserialize(const std::string_view);
};

} // namespace meta
} // namespace nebula
