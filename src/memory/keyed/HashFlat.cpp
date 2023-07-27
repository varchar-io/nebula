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

#include "HashFlat.h"

#include "common/Wrap.h"
#include "surface/eval/UDF.h"

namespace nebula {
namespace memory {
namespace keyed {

using Range = nebula::common::PRange;
using nebula::common::OneSlice;
using nebula::common::vector_reserve;
using nebula::surface::eval::Aggregator;
using nebula::surface::eval::Sketch;
using nebula::type::Kind;
using nebula::type::TypeTraits;

void HashFlat::init() {
  vector_reserve(ops_, numColumns_, "HashFlat::init");
  keys_.reserve(numColumns_);
  values_.reserve(numColumns_);

  for (size_t i = 0; i < numColumns_; ++i) {
    // every column may have its own operations
    ops_.emplace_back(genComparator(i), genHasher(i), genCopier(i));
    if (!isAggregate(i)) {
      keys_.push_back(i);
    } else {
      values_.push_back(i);
    }
  }

  // reserve a memory chunk to store hash values
  if (keys_.size() > 0) {
    keyHash_ = std::make_unique<OneSlice>(sizeof(size_t) * keys_.size());

    // optimization - if the keys are all primmitive (non-strings), and index are continous
    // we can just hash the memory chunk (row.offset+col-offset, total-size)
    size_t last = keys_.at(0) - 1;
    optimal_ = true;
    keyWidth_ = 0;
    for (auto index : keys_) {
      Kind k = cops_.at(index).kind;
      keyWidth_ += cops_.at(index).width;
      if (index != last + 1 || k >= Kind::VARCHAR) {
        optimal_ = false;
        break;
      }

      // assign last for next check
      last = index;
    }
  }
}

Comparator HashFlat::genComparator(size_t i) noexcept {
  // only need for key
  if (isAggregate(i)) {
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
  Kind k = cops_.at(i).kind;
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
      auto r1 = Range::make(main_->slice, row1Offset + colProps1.offset);
      auto r2 = Range::make(main_->slice, row2Offset + colProps2.offset);

      // length has to be the same
      if (r1.size != r2.size) {
        return r1.size - r2.size;
      }

      return data_->slice.compare(r1.offset, r2.offset, r1.size);
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
  // only need for key
  if (isAggregate(i)) {
    return {};
  }

#define PREPARE_AND_NULL()                        \
  const auto& rowProps = rows_.at(row);           \
  auto rowOffset = rowProps.offset;               \
  const auto& colProps = rowProps.colProps.at(i); \
  if (colProps.isNull) {                          \
    return 0L;                                    \
  }

#define TYPE_HASH(KIND, TYPE)                                      \
  case Kind::KIND: {                                               \
    return [this, i](size_t row) -> size_t {                       \
      PREPARE_AND_NULL()                                           \
      return main_->slice.hash<TYPE>(rowOffset + colProps.offset); \
    };                                                             \
  }

  // fetch value
  Kind k = cops_.at(i).kind;
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
    return [this, i](size_t row) -> size_t {
      PREPARE_AND_NULL()

      auto so = rowOffset + colProps.offset;
      auto r = Range::make(main_->slice, so);

      // read the real data from data_
      // TODO(cao) - we don't need convert strings from bytes for hash
      // instead, slice should be able to hash the range[offset, len] much cheaper
      if (r.size == 0) {
        return 0L;
      }

      return data_->slice.hash(r.offset, r.size);
    };
  }
  default:
    return [i](size_t) -> size_t {
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
Copier HashFlat::genCopier(size_t i) noexcept {
  // only need for value
  if (!isAggregate(i)) {
    return {};
  }

  const auto& f = fields_.at(i);
  const auto ot = f->outputType();
  const auto it = f->inputType();

// below provides a template to write core logic for all input / output type combinations
#define LOGIC_BY_IO(O, I) \
  case Kind::I: return bind<Kind::O, Kind::I>(i);

  ITERATE_BY_IO(ot, it)

#undef LOGIC_BY_IO

  // no supported combination found
  return {};
}

std::pair<size_t, size_t> HashFlat::optimalKeys(size_t row) const noexcept {
  const auto& rowProps = rows_.at(row);

  // starting offset of all sequential keys = row offset + first key offset
  auto offset = rowProps.offset + rowProps.colProps.at(0).offset;
  auto length = keyWidth_;

  // remove nulls
  for (auto index : keys_) {
    const auto& colProps = rowProps.colProps.at(index);
    if (colProps.isNull) {
      length -= cops_.at(index).width;
    }
  }

  return { offset, length };
}

// compute hash value of given row and column list
// The function has very similar logic as row accessor, we inline it for perf
size_t HashFlat::hash(size_t rowId) const {
  // if optimal, let's figure out offset and length of the key bytes
  if (N_LIKELY(optimal_)) {
    auto kp = optimalKeys(rowId);
    auto ptr = main_->slice.ptr();
    // just hash the key set
    return nebula::common::Hasher::hash64(ptr + kp.first, kp.second);
  }

  // hash on every column and write value into keyHash_ chunk
  if (N_LIKELY(keyHash_ != nullptr)) {
    size_t* ptr = (size_t*)keyHash_->ptr();
    auto len = keyHash_->size();
    std::memset(ptr, 0, len);

    for (size_t i = 0, size = keys_.size(); i < size; ++i) {
      *(ptr + i) = ops_.at(keys_.at(i)).hasher(rowId);
    }

    return nebula::common::Hasher::hash64(ptr, len);
  }

  return 0;
}

// check if two rows are equal to each other on given columns
bool HashFlat::equal(size_t row1, size_t row2) const {
  // if optimal, let's figure out offset and length of the key bytes
  if (N_LIKELY(optimal_)) {
    auto kp1 = optimalKeys(row1);
    auto kp2 = optimalKeys(row2);
    // if length equals - return false
    if (kp1.second != kp2.second) {
      return false;
    }

    auto ptr = main_->slice.ptr();
    return std::memcmp(ptr + kp1.first, ptr + kp2.first, kp1.second) == 0;
  }

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
  rowKeys_.insert(key);

  // since this is a new row, create aggregator for all its value fields
  auto& rowProps = rows_.at(newRow);
  for (size_t i : values_) {
    auto& sketch = rowProps.colProps.at(i).sketch;
    if (sketch == nullptr) {
      sketch = cops_.at(i).sketcher();
      // since this is the first time sketch created, merge its own value
      ops_.at(i).copier(newRow, newRow);
    }
  }

  return false;
}

} // namespace keyed
} // namespace memory
} // namespace nebula