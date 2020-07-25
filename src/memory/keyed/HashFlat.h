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

#include "common/Hash.h"
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
using Hasher = std::function<size_t(size_t)>;

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
    : FlatBuffer(schema, fields),
      keyHash_{ nullptr },
      optimal_{ false } {
    init();
  }

  HashFlat(FlatBuffer* in,
           const nebula::surface::eval::Fields& fields)
    : FlatBuffer(in->schema(), fields, (NByte*)in->chunk()),
      keyHash_{ nullptr },
      optimal_{ false } {
    init();
  }

  virtual ~HashFlat() = default;

  // compute hash value of given row and column list
  size_t hash(size_t rowId) const;

  // check if two rows are equal to each other on given columns
  bool equal(size_t row1, size_t row2) const;

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

  // merge template to merge row1 into row2
  template <nebula::type::Kind O, nebula::type::Kind I>
  void merge(size_t row1, size_t row2, size_t i) {
    using InputType = typename nebula::type::TypeTraits<I>::CppType;
    const auto& row1Props = rows_.at(row1);
    const auto& row2Props = rows_.at(row2);
    const auto& colProps1 = row1Props.colProps.at(i);
    auto& colProps2 = row2Props.colProps.at(i);
    N_ENSURE_NOT_NULL(colProps2.sketch, "merge row should have sketch");
    if (UNLIKELY(row1 != row2 && colProps1.sketch != nullptr)) {
      colProps2.sketch->mix(*colProps1.sketch);
      return;
    }
    if (colProps1.isNull) {
      return;
    }
    InputType value = main_->slice.read<InputType>(row1Props.offset + colProps1.offset);
    auto agg = std::static_pointer_cast<nebula::surface::eval::Aggregator<O, I>>(colProps2.sketch);
    agg->merge(value);
  }

  template <nebula::type::Kind O>
  void merge_string(size_t row1, size_t row2, size_t i) {
    const auto& row1Props = rows_.at(row1);
    const auto& row2Props = rows_.at(row2);
    const auto& colProps1 = row1Props.colProps.at(i);
    auto& colProps2 = row2Props.colProps.at(i);
    N_ENSURE_NOT_NULL(colProps2.sketch, "merge row should have sketch");
    if (UNLIKELY(row1 != row2 && colProps1.sketch != nullptr)) {
      colProps2.sketch->mix(*colProps1.sketch);
      return;
    }
    if (colProps1.isNull) {
      return;
    }
    std::string_view value = read(row1Props.offset, colProps1.offset);
    auto agg = std::static_pointer_cast<nebula::surface::eval::Aggregator<O, nebula::type::Kind::VARCHAR>>(colProps2.sketch);
    agg->merge(value);
  }

  template <nebula::type::Kind O, nebula::type::Kind I>
  Copier bind(size_t col) {
    if constexpr (I == nebula::type::Kind::VARCHAR) {
      return std::bind(&HashFlat::merge_string<O>, this, std::placeholders::_1, std::placeholders::_2, col);
    } else {
      return std::bind(&HashFlat::merge<O, I>, this, std::placeholders::_1, std::placeholders::_2, col);
    }
  }

  // in optimal case: where given keys are layout in main slice sequentially
  std::pair<size_t, size_t> optimalKeys(size_t) const noexcept;

private:
  // lay all hash values in this fixed slice
  std::unique_ptr<nebula::common::OneSlice> keyHash_;
  // optimal indicates the hash/compare/copy on keys can be optimized
  // since will be laid out sequentially in main slice only
  bool optimal_;
  // expected width for all keys when optimal and no nulls
  // calculated when optimal is true
  size_t keyWidth_;
  std::vector<size_t> keys_;
  std::vector<size_t> values_;
  // customized operations for each column
  std::vector<ColOps> ops_;

  // TODO(cao):
  // build error Undefined symbols for architecture x86_64: "folly::f14::detail::F14LinkCheck
  // https://engineering.fb.com/developer-tools/f14/
  // folly::F14FastSet<Key, Hash, Equal> rowKeys_;
  // set max load factor as 0.6 to reduce rehash - default 0.8
  nebula::common::unordered_set<Key, Hash, Equal, 60> rowKeys_;
};

} // namespace keyed
} // namespace memory
} // namespace nebula