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
  using Key = std::tuple<HashFlat&, size_t>;

public:
  HashFlat(const nebula::type::Schema schema,
           const std::vector<size_t>& keys)
    : FlatBuffer(schema), keys_{ std::move(keys) } {}

  virtual ~HashFlat() = default;

  // update a row in hash flat, if same key existings, update the row and return true
  // otherwise we get a new row, return false
  bool update(nebula::surface::RowData&, const UpdateCallback&);

  struct Hash {
    size_t operator()(const Key& row) const {
      auto& flat = std::get<0>(row);
      auto rowId = std::get<1>(row);

      // calculate the specified row's hash based on its keys
      auto size = flat.rowHash_.size();
      if (rowId < size) {
        return flat.rowHash_.at(rowId);
      }

      // for every key column
      return flat.hash(rowId, flat.keys_);
    }
  };

  struct Equal {
    bool operator()(const Key& row1, const Key& row2) const {
      auto& flat = std::get<0>(row1);
      auto rowId1 = std::get<1>(row1);
      auto rowId2 = std::get<1>(row2);
      // two rows equal if they have the same keysƒ
      return flat.equal(rowId1, rowId2, flat.keys_);
    }
  };

private:
  std::vector<size_t> keys_;

  // rowHash_ is used to cache hash
  std::vector<size_t> rowHash_;

  // TODO(cao):
  // build error Undefined symbols for architecture x86_64: "folly::f14::detail::F14LinkCheck
  // folly::F14FastMap<Key, size_t, Hash, Equal> keyedRows_;
  std::unordered_map<Key, size_t, Hash, Equal> keyedRows_;
};
} // namespace keyed
} // namespace memory
} // namespace nebula