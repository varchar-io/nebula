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

namespace nebula {
namespace memory {
namespace keyed {
/**
* Hash flat is using flat buffer to build up a data set with specified keys. 
* And, hash flat will not allow duplicate keys in the data set.
*/

class HashFlat : public FlatBuffer {
  // key is tuple of hash flat object, row id, row hash (by keys)
  using Key = std::tuple<HashFlat&, size_t, size_t>;

public:
  HashFlat(const nebula::type::Schema schema,
           const std::vector<size_t>& keys)
    : FlatBuffer(schema), keys_{ std::move(keys) } {}

  virtual ~HashFlat() = default;

  // update a row in hash flat, if same key existings, update the row and return true
  // otherwise we get a new row, return false
  bool update(const nebula::surface::RowData&, const UpdateCallback&);

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
      return flat.equal(std::get<1>(row1), std::get<1>(row2), flat.keys_);
    }
  };

private:
  std::vector<size_t> keys_;

  // rowHash_ is used to cache hash
  // std::vector<size_t> rowHash_;

  // TODO(cao):
  // build error Undefined symbols for architecture x86_64: "folly::f14::detail::F14LinkCheck
  // https://engineering.fb.com/developer-tools/f14/
  // folly::F14FastSet<Key, Hash, Equal> rowKeys_;
  std::unordered_set<Key, Hash, Equal> rowKeys_;
};
} // namespace keyed
} // namespace memory
} // namespace nebula