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

#include "HashFlat.h"

#include "surface/eval/UDF.h"

namespace nebula {
namespace memory {
namespace keyed {

using nebula::type::Kind;

void HashFlat::init() {
  values_.reserve(numColumns_ - keys_.size());
  ops_.reserve(numColumns_);

  for (size_t i = 0; i < numColumns_; ++i) {
    if (keys_.find(i) == keys_.end()) {
      values_.emplace(i);
    }

    // every column may have its own operations
    ops_.emplace_back(genComparator(i), genHasher(i), genCopier(i));
  }

  // set a max load factor to reduce rehash
  rowKeys_.max_load_factor(0.5);
}

Comparator HashFlat::genComparator(size_t i) noexcept {
  // only need for key
  if (keys_.find(i) == keys_.end()) {
    return {};
  }

#define PREPARE_AND_NULLCHECK()                     \
  const auto& row1Props = rows_[row1];              \
  const auto& row2Props = rows_[row2];              \
  auto row1Offset = row1Props.offset;               \
  auto row2Offset = row2Props.offset;               \
  const auto& colProps1 = row1Props.colProps.at(i); \
  const auto& colProps2 = row2Props.colProps.at(i); \
  if (colProps1.isNull != colProps2.isNull) {       \
    return -1;                                      \
  }                                                 \
  if (colProps1.isNull) {                           \
    return 0;                                       \
  }

#define TYPE_COMPARE(KIND, TYPE)                                                                       \
  case Kind::KIND: {                                                                                   \
    return [this, i](size_t row1, size_t row2) -> int {                                                \
      PREPARE_AND_NULLCHECK()                                                                          \
      return main_->slice.compare<TYPE>(row1Offset + colProps1.offset, row2Offset + colProps2.offset); \
    };                                                                                                 \
  }

  // fetch value
  Kind k = kw_.at(i).first;
  // we only support primitive types for keys
  switch (k) {
    TYPE_COMPARE(BOOLEAN, bool)
    TYPE_COMPARE(TINYINT, int8_t)
    TYPE_COMPARE(SMALLINT, int16_t)
    TYPE_COMPARE(INTEGER, int32_t)
    TYPE_COMPARE(BIGINT, int64_t)
    TYPE_COMPARE(REAL, int32_t)
    TYPE_COMPARE(DOUBLE, int64_t)
    TYPE_COMPARE(INT128, int128_t)
  case Kind::VARCHAR: {
    // read the real data from data_
    // TODO(cao) - we don't need convert strings from bytes for hash
    // instead, slice should be able to compare two byte range
    return [this, i](size_t row1, size_t row2) -> int {
      PREPARE_AND_NULLCHECK()

      // offset and length of each
      auto o1 = row1Offset + colProps1.offset;
      auto s1Length = main_->slice.read<int32_t>(o1 + 4);
      auto o2 = row2Offset + colProps2.offset;
      auto s2Length = main_->slice.read<int32_t>(o2 + 4);

      // length has to be the same
      if (s1Length != s2Length) {
        return s1Length - s2Length;
      }

      auto s1Offset = main_->slice.read<int32_t>(o1);
      auto s2Offset = main_->slice.read<int32_t>(o2);

      return data_->slice.compare(s1Offset, s2Offset, s1Length);
    };
  }
  default:
    return [i](size_t, size_t) -> int {
      LOG(ERROR) << "Compare a non-supported column: " << i;
      return 0;
    };
  }

#undef PREPARE_AND_NULLCHECK
#undef TYPE_COMPARE
}

Hasher HashFlat::genHasher(size_t i) noexcept {
  static constexpr size_t flip = 0x3600ABC35871E005UL;

  // only need for key
  if (keys_.find(i) == keys_.end()) {
    return {};
  }

  // TODO(why do we hash these bytes instead using its own value?)
#define PREPARE_AND_NULL()                        \
  const auto& rowProps = rows_.at(row);           \
  auto rowOffset = rowProps.offset;               \
  const auto& colProps = rowProps.colProps.at(i); \
  if (colProps.isNull) {                          \
    return (hash ^ flip) >> 32;                   \
  }

#define TYPE_HASH(KIND, TYPE)                                        \
  case Kind::KIND: {                                                 \
    return [this, i](size_t row, size_t hash) {                      \
      PREPARE_AND_NULL()                                             \
      auto h = main_->slice.hash<TYPE>(rowOffset + colProps.offset); \
      return (hash ^ h) >> 32;                                       \
    };                                                               \
  }

  // fetch value
  Kind k = kw_.at(i).first;
  switch (k) {
    TYPE_HASH(BOOLEAN, bool)
    TYPE_HASH(TINYINT, int8_t)
    TYPE_HASH(SMALLINT, int16_t)
    TYPE_HASH(INTEGER, int32_t)
    TYPE_HASH(BIGINT, int64_t)
    TYPE_HASH(REAL, int32_t)
    TYPE_HASH(DOUBLE, int64_t)
    TYPE_HASH(INT128, int128_t)
  case Kind::VARCHAR: {
    // read 4 bytes offset and 4 bytes length
    return [this, i](size_t row, size_t hash) {
      PREPARE_AND_NULL()

      auto so = rowOffset + colProps.offset;
      auto offset = main_->slice.read<int32_t>(so);
      auto len = main_->slice.read<int32_t>(so + 4);

      // read the real data from data_
      // TODO(cao) - we don't need convert strings from bytes for hash
      // instead, slice should be able to hash the range[offset, len] much cheaper
      if (len == 0) {
        return hash;
      }

      auto h = data_->slice.hash(offset, len);
      return (hash ^ h) >> 32;
    };
  }
  default:
    return [i](size_t, size_t) -> size_t {
      LOG(ERROR) << "Hash a non-supported column: " << i;
      return 0;
    };
  }

#undef PREPARE_AND_NULL
#undef TYPE_HASH
}

// TODO(cao): I spent a couple of days trying to nail down which GCC optimization
// causing the SIGSEGV on the returned function from this method
// CMakeList.txt has more details in the problem.
// So now, we disable optimizations higher than level 1 on this method
// we may dig more in the future to understand this problem better. (CLANG need no this)
#ifndef __clang__
__attribute__((optimize("O1")))
#endif
Copier
  HashFlat::genCopier(size_t i) noexcept {
  // only need for value
  if (keys_.find(i) != keys_.end()) {
    return {};
  }

  // TODO(cao): ensure target to be null - generic way handle null metrics?
#define UPDATE_COLUMN(KIND, TYPE)                            \
  case Kind::KIND: {                                         \
    return [this, i](size_t row1, size_t row2) {             \
      const auto& row1Props = rows_.at(row1);                \
      const auto& row2Props = rows_.at(row2);                \
      const auto& colProps1 = row1Props.colProps.at(i);      \
      const auto& colProps2 = row2Props.colProps.at(i);      \
      if (colProps1.isNull) {                                \
        return;                                              \
      }                                                      \
      auto row2Offset = row2Props.offset + colProps2.offset; \
      auto row1Offset = row1Props.offset + colProps1.offset; \
      TYPE nv = main_->slice.read<TYPE>(row1Offset);         \
      TYPE ov = main_->slice.read<TYPE>(row2Offset);         \
      TYPE x = fields_.at(i)->merge(ov, nv);                 \
      main_->slice.write<TYPE>(row2Offset, x);               \
    };                                                       \
  }

  Kind k = kw_.at(i).first;
  // we only support primitive types aggregation fields
  switch (k) {
    UPDATE_COLUMN(BOOLEAN, bool)
    UPDATE_COLUMN(TINYINT, int8_t)
    UPDATE_COLUMN(SMALLINT, int16_t)
    UPDATE_COLUMN(INTEGER, int32_t)
    UPDATE_COLUMN(BIGINT, int64_t)
    UPDATE_COLUMN(REAL, float)
    UPDATE_COLUMN(DOUBLE, double)
    UPDATE_COLUMN(INT128, int128_t)
  default:
    return [i](size_t, size_t) {
      LOG(ERROR) << "This column can not be copy-updated: " << i;
    };
  }

#undef UPDATE_COLUMN
}

// compute hash value of given row and column list
// The function has very similar logic as row accessor, we inline it for perf
size_t HashFlat::hash(size_t rowId) const {
  static constexpr size_t start = 0xC6A4A7935BD1E995UL;
  size_t hvalue = start;

  // hash on every column
  for (auto index : keys_) {
    hvalue = ops_.at(index).hasher(rowId, hvalue);
  }

  return hvalue;
}

// check if two rows are equal to each other on given columns
bool HashFlat::equal(size_t row1, size_t row2) const {
  for (auto index : keys_) {
    if (ops_.at(index).comparator(row1, row2) != 0) {
      return false;
    }
  }

  return true;
}

bool HashFlat::update(const nebula::surface::RowData& row) {
  // add a new row to the buffer may be expensive
  // if there are object values to be created such as customized aggregation
  // to have consistent way - we're taking this approach
  // adding keys only
  // TODO: may not be correct approach
  // this->add(row, keys_);
  this->add(row);

  auto newRow = getRows() - 1;
  auto hValue = hash(newRow);
  Key key{ *this, newRow, hValue };
  auto itr = rowKeys_.find(key);
  if (itr != rowKeys_.end()) {
    // copy the new row data into target for non-keys
    auto oldRow = std::get<1>(*itr);
    for (size_t i : values_) {
      ops_.at(i).copier(newRow, oldRow);
    }

    // rollback the new added row
    rollback();

    return true;
  }

  // resume all values population and add a new row key
  // this->resume(row, values_, newRow);
  rowKeys_.insert(key);
  return false;
}

} // namespace keyed
} // namespace memory
} // namespace nebula