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

#include "Partition.h"

#include "common/Hash.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * Cube is exactly the same thing but with static interface. Pod supports dynamic interface.
 * Here, we're using Pod to describe data partitioning in node, means for the same table on any given node
 * we may end up with many pods which is also known as block in the system.
 * 
 * Pods can be merged across nodes in a background process.
 * 
 */
namespace nebula {
namespace meta {

using nebula::type::Schema;

// TODO(cao): is int32 enough to hold all bits for each bess value?
// defintely not if there are huge list of dimensions to encode
using BessType = int32_t;

// a cube defines a multi-dimensional space.
// it accepts a tuple of partition keys
// and provides all function mapping on these dimension definitions
class Pod {
public:
  using KeyList = std::vector<std::unique_ptr<PK>>;
  Pod(KeyList keys) : keys_{ std::move(keys) } {
    // caculate total combination of possible blocks
    // we're doing multi-dimension (spaces) mapping to single dimension (partition ID)
    // {S1, S2, S3} => Pi, for example, P = S3*(Spaces(2)*Spaces(1))+S2*Spaces(1)+S1
    const auto N = keys_.size();
    offsets_.reserve(N);
    shifts_.reserve(N);
    auto offset = 1;
    auto shift = 0;
    for (size_t i = 0, c = keys_.size(); i < c; ++i) {
      offsets_.emplace_back(offset);
      shifts_.emplace_back(shift);

      const auto& key = keys_.at(i);
      offset *= key->spaces();
      shift += key->width();
      colMap_[key->name()] = i;
    }

    capacity_ = offset;
    bessBits_ = shift;
  }

  // max number of blocks = product of (Ki spaces)
  size_t capacity() const {
    return capacity_;
  }

  inline size_t numKeys() const {
    return keys_.size();
  }

  inline size_t offset(size_t index) const {
    return offsets_.at(index);
  }

#define POD_COMPUTE(P)                               \
  bess = 0;                                          \
  size_t pod = 0;                                    \
  for (size_t i = 0, c = keys_.size(); i < c; ++i) { \
    auto p = keys_.at(i)->slot(P);                   \
    pod += offsets_[i] * p.space;                    \
    bess |= p.offset << shifts_[i];                  \
  }                                                  \
  return pod;

  inline size_t pod(const std::vector<std::string>& vs, BessType& bess) const {
    POD_COMPUTE(vs.at(i))
  }

  inline size_t pod(const nebula::surface::RowData& row, BessType& bess) const {
    POD_COMPUTE(row)
  }

#undef POD_COMPUTE

  // if a field is a partitioned column, parse the encoded value from its bess value
  // return true if column is found, otherwise return false indicating this is not a partition column
  template <typename T>
  inline bool value(const std::string& name,
                    const std::vector<size_t>& spaces,
                    BessType bess,
                    T& v) {
    auto itr = colMap_.find(name);

    // not found
    if (itr == colMap_.end()) {
      return false;
    }

    // parse the value
    auto i = itr->second;
    v = keys_.at(i)->value<T>((bess >> shifts_[i]), spaces.at(i));
    return true;
  }

  template <typename T>
  inline std::vector<T> values(const std::string& name, const std::vector<size_t>& spaces) {
    auto itr = colMap_.find(name);

    // not found
    if (itr == colMap_.end()) {
      return {};
    }

    // get the value range for given space
    auto i = itr->second;
    return keys_.at(i)->values<T>(spaces.at(i));
  }

  inline Span span(const std::vector<std::vector<std::string>>& ps) const {
    Span span{ 0, 0 };
    for (size_t i = 0, c = keys_.size(); i < c; ++i) {
      span = span + keys_.at(i)->range(ps.at(i)) * offsets_[i];
    }
    return span;
  }

  // we can find out each space for any dimension for any given ID
  // take 3 dimensions as example, their max spaces are {S1, S2, S3}
  // Given P = K1*1 + K2*S1 + K3*(S1*S2)
  // =>
  //   K3 = P/(S1 * S2)
  //   K2 = (P - K3 * S1 * S2) / S1
  //   K1 = (P - K3*S1*S2 - K2*S1)
  inline std::vector<size_t> locate(size_t pid) const {
    std::vector<size_t> spaces(numKeys(), 0);

    // from last item
    auto value = pid;
    for (size_t i = numKeys(); i > 0; --i) {
      auto index = i - 1;
      auto offset = offsets_.at(index);
      auto space = value / offset;
      value -= space * offset;

      // save space value for current dimension
      spaces[index] = space;
    }

    return spaces;
  }

  // total bytes needed to store each bess value
  inline size_t bessBits() const {
    return bessBits_;
  }

private:
  KeyList keys_;
  // column name to index mapping for keys
  nebula::common::unordered_map<std::string, size_t> colMap_;
  // offsets to compute PID
  std::vector<size_t> offsets_;
  // shifts for each key to compute BESS
  std::vector<size_t> shifts_;
  // bess width = sum of shifts_
  size_t bessBits_;
  size_t capacity_;
};

} // namespace meta
} // namespace nebula