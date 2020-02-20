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

// #include <folly/container/F14Set.h>
#include <unordered_set>

#include "FlatBuffer.h"
#include "surface/DataSurface.h"
#include "surface/eval/ValueEval.h"

namespace nebula {
namespace memory {
namespace keyed {
/**
* Hash flat is using flat buffer to build up a data set with specified keys. 
* And, hash flat will not allow duplicate keys in the data set.
*/

// column comparator between two rows
using Comparator = std::function<int32_t(size_t, size_t)>;

// Hasher on one column of given row and base hash value
using Hasher = std::function<size_t(size_t, size_t)>;

// Copier on one column from given row1 to row2 which using external updater
using Copier = std::function<void(size_t, size_t)>;

struct ColOps {
  explicit ColOps(Comparator c, Hasher h, Copier o)
    : comparator{ std::move(c) },
      hasher{ std::move(h) },
      copier{ std::move(o) } {}

  Comparator comparator;
  Hasher hasher;
  Copier copier;
};

class HashFlat : public FlatBuffer {
  // key is tuple of hash flat object, row id, row hash (by keys)
  using Key = std::tuple<HashFlat&, size_t, size_t>;

public:
  HashFlat(const nebula::type::Schema schema,
           const nebula::surface::eval::Fields& fields)
    : FlatBuffer(schema, fields) {
    init();
  }

  HashFlat(FlatBuffer* in,
           const nebula::surface::eval::Fields& fields)
    : FlatBuffer(in->schema(), fields, (NByte*)in->chunk()) {
    init();
  }

  virtual ~HashFlat() = default;

  // compute hash value of given row and column list
  size_t hash(size_t rowId) const;

  // check if two rows are equal to each other on given columns
  bool equal(size_t row1, size_t row2) const;

  // copy data of row1 into row2
  bool copy(size_t row1, size_t row2);

  // update a row in hash flat, if same key existings, update the row and return true
  // otherwise we get a new row, return false
  bool update(const nebula::surface::RowData&);

  struct Hash {
    inline size_t operator()(const Key& key) const noexcept {
      return std::get<2>(key);
    }
  };

  struct Equal {
    bool operator()(const Key& row1, const Key& row2) const noexcept {
      // both keys have to coming from the same flat object
      auto& flat = std::get<0>(row1);
      // two rows equal if they have the same keys
      return flat.equal(std::get<1>(row1), std::get<1>(row2));
    }
  };

private:
  void init();
  Comparator genComparator(size_t) noexcept;
  Hasher genHasher(size_t) noexcept;
  Copier genCopier(size_t) noexcept;

private:
  std::unordered_set<size_t> keys_;
  std::unordered_set<size_t> values_;
  // customized operations for each column
  std::vector<ColOps> ops_;

  // TODO(cao):
  // build error Undefined symbols for architecture x86_64: "folly::f14::detail::F14LinkCheck
  // https://engineering.fb.com/developer-tools/f14/
  // folly::F14FastSet<Key, Hash, Equal> rowKeys_;
  std::unordered_set<Key, Hash, Equal> rowKeys_;
};
} // namespace keyed
} // namespace memory
} // namespace nebula