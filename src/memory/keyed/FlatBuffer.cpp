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

#include "FlatBuffer.h"

namespace nebula {
namespace memory {
namespace keyed {

using nebula::common::PagedSlice;
using nebula::surface::ListData;
using nebula::surface::RowData;
using nebula::type::Kind;

FlatBuffer::FlatBuffer(const nebula::type::Schema& schema)
  : schema_{ schema },
    main_{ 1024 },
    data_{ 1024 },
    list_{ 1024 } {
  // build name to index look up
  // build a field name to data node
  const auto numCols = schema_->size();
  fields_.reserve(numCols);
  widths_.reserve(numCols);

  for (size_t i = 0; i < numCols; ++i) {
    auto f = schema_->childType(i);
    fields_[f->name()] = i;

    widths_[i] = widthInMain(f->k());
  }
}

size_t FlatBuffer::widthInMain(Kind kind) {
#define SCALAR_WIDTH_DISTR(KIND)                        \
  case Kind::KIND: {                                    \
    return nebula::type::TypeTraits<Kind::KIND>::width; \
  }

  // add solid values for each type
  switch (kind) {
    SCALAR_WIDTH_DISTR(BOOLEAN)
    SCALAR_WIDTH_DISTR(TINYINT)
    SCALAR_WIDTH_DISTR(SMALLINT)
    SCALAR_WIDTH_DISTR(INTEGER)
    SCALAR_WIDTH_DISTR(BIGINT)
    SCALAR_WIDTH_DISTR(REAL)
    SCALAR_WIDTH_DISTR(DOUBLE)
  case Kind::VARCHAR: {
    // 4 bytes offset + 4 bytes length
    return 8;
  }
  case Kind::ARRAY: {
    // 4 bytes number of items + 4 bytes offset in list
    return 8;
  }
  default:
    throw NException("other types not supported yet");
  }

#undef SCALAR_WIDTH_DISTR
}

bool FlatBuffer::appendNull(bool isNull, nebula::type::Kind kind, Buffer& dest) {
  int8_t byte = (isNull ? HIGH6_1 : 0) | kind;
  N_ENSURE_EQ(((byte & HIGH6_1) != 0), isNull, "HIGH6 byte stores null");

  size_t len = dest.slice.write<int8_t>(dest.offset, byte);
  N_ENSURE_EQ(len, 1, "length to be one");

  dest.offset += 1;
  return isNull;
}

template <typename T>
size_t FlatBuffer::appendScalar(T value, Buffer& dest) {
  size_t len = dest.slice.write(dest.offset, value);
  dest.offset += len;

  // return current offset
  return dest.offset;
}

size_t FlatBuffer::appendString(const std::string& str, Buffer& dest) {
  // write variable data into dirty buffer and get offset and length
  auto len = data_.slice.write(data_.offset, str.data(), str.size());

  // write offset and length in main chunk
  appendScalar<int32_t>(data_.offset, dest);
  appendScalar<int32_t>(len, dest);

  // move forward data offset for next
  data_.offset += len;

  return len;
}

// TODO(cao) - right now, we're adding list at first level buffer
// which makes it's impossible to update list type in place.
// If we want to support updating list, we should move its data to next level (like string).
// We can considering using a dynamic chained buffers to allow multiple levels of dirty buffers.
size_t FlatBuffer::appendList(Kind itemKind, std::unique_ptr<ListData> list) {
  auto offset = list_.offset;
  auto count = list->getItems();

  // add every single item here, the function is similar to add method
  // which iterates over columns instead
  for (auto i = 0; i < count; ++i) {
    if (appendNull(list->isNull(i), itemKind, list_)) {
      // add null for this field
      continue;
    }

#define SCALAR_DATA_DISTR(KIND, FUNC)   \
  case Kind::KIND: {                    \
    appendScalar(list->FUNC(i), list_); \
    break;                              \
  }

    // add solid values for each type
    switch (itemKind) {
      SCALAR_DATA_DISTR(BOOLEAN, readBool)
      SCALAR_DATA_DISTR(TINYINT, readByte)
      SCALAR_DATA_DISTR(SMALLINT, readShort)
      SCALAR_DATA_DISTR(INTEGER, readInt)
      SCALAR_DATA_DISTR(BIGINT, readLong)
      SCALAR_DATA_DISTR(REAL, readFloat)
      SCALAR_DATA_DISTR(DOUBLE, readDouble)
    case Kind::VARCHAR: {
      appendString(list->readString(i), list_);
      break;
    }
    default:
      throw NException("other types not supported yet");
    }

#undef SCALAR_DATA_DISTR
  }

  return offset;
}

size_t FlatBuffer::rollback() {
  // has last row to roll back
  size_t size = 0;
  if (rows_.size() > 0) {
    // size of the last row to be roll back
    size = std::get<1>(rows_.back());

    // remove last row
    rows_.pop_back();

    // every buffer reset offset
    main_.offset = std::get<0>(last_);
    data_.offset = std::get<1>(last_);
    list_.offset = std::get<2>(last_);
  }

  return size;
}

// add a row into current batch
size_t FlatBuffer::add(const nebula::surface::RowData& row) {
  // record current state before adding a new row - used for rollback
  last_ = std::make_tuple(main_.offset, data_.offset, list_.offset);

  const auto rowOffset = main_.offset;
  const auto numColumns = schema_->size();

  std::vector<bool> nulls;
  nulls.reserve(numColumns);
  for (size_t i = 0; i < numColumns; ++i) {
    auto type = schema_->childType(i);
    const Kind kind = type->k();
    nulls.push_back(appendNull(row.isNull(i), kind, main_));
  }

#define SCALAR_DATA_DISTR(KIND, FUNC) \
  case Kind::KIND: {                  \
    appendScalar(row.FUNC(i), main_); \
    break;                            \
  }

  // handle every column
  for (size_t i = 0; i < numColumns; ++i) {

    // if it is null, do nothing
    if (nulls[i]) {
      continue;
    }

    auto type = schema_->childType(i);
    const Kind kind = type->k();
    // add solid values for each type
    switch (kind) {
      SCALAR_DATA_DISTR(BOOLEAN, readBool)
      SCALAR_DATA_DISTR(TINYINT, readByte)
      SCALAR_DATA_DISTR(SMALLINT, readShort)
      SCALAR_DATA_DISTR(INTEGER, readInt)
      SCALAR_DATA_DISTR(BIGINT, readLong)
      SCALAR_DATA_DISTR(REAL, readFloat)
      SCALAR_DATA_DISTR(DOUBLE, readDouble)
    case Kind::VARCHAR: {
      appendString(row.readString(i), main_);
      break;
    }
    case Kind::ARRAY: {
      auto list = row.readList(i);

      // write 4 bytes of N = number of items
      appendScalar<int32_t>(list->getItems(), main_);
      auto listType = std::dynamic_pointer_cast<nebula::type::ListType>(type);
      auto childKind = listType->childType(0)->k();
      auto listOffset = appendList(childKind, std::move(list));
      appendScalar<int32_t>(listOffset, main_);

      break;
    }
    default:
      throw NException("other types not supported yet");
    }
  }
#undef SCALAR_DATA_DISTR

  // after processing all columns, we got the row offset and length, record it here
  rows_.push_back(std::make_tuple(rowOffset, main_.offset - rowOffset));

  return rowOffset;
}

// random access to a row - may require internal seek
const std::unique_ptr<RowData> FlatBuffer::crow(size_t rowId) const {
  auto& rowProp = rows_[rowId];
  auto rowOffset = std::get<0>(rowProp);
  // calculate each column: null or not, offset,

  return std::make_unique<RowAccessor>(*this, rowOffset, columnProps(rowOffset));
}

const RowData& FlatBuffer::row(size_t rowId) {
  auto& rowProp = rows_[rowId];
  auto rowOffset = std::get<0>(rowProp);
  // calculate each column: null or not, offset,

  current_ = std::make_unique<RowAccessor>(*this, rowOffset, columnProps(rowOffset));
  return *current_;
}

FlatColumnProps FlatBuffer::columnProps(size_t offset) const {
  auto numCols = schema_->size();
  FlatColumnProps colProps;
  colProps.reserve(numCols);

  // first numCols bytes store nulls for all columns
  auto colOffset = numCols;
  for (size_t i = 0; i < numCols; ++i) {
    auto nullByte = main_.slice.read<int8_t>(offset + i);
    Kind kind = (Kind)(nullByte & ~HIGH6_1);
    bool nv = (nullByte & HIGH6_1) != 0;
    colProps.emplace_back(nv, colOffset, kind);
    if (!nv) {
      colOffset += widths_[i];
    }
  }

  return colProps;
}

// compute hash value of given row and column list
// The function has very similar logic as row accessor, we inline it for perf
size_t FlatBuffer::hash(size_t rowId, const std::vector<size_t>& cols) const {
  auto rowOffset = std::get<0>(rows_[rowId]);
  FlatColumnProps colProps = columnProps(std::get<0>(rows_[rowId]));
  static constexpr size_t flip = 0x3600ABC35871E005UL;
  static constexpr size_t start = 0xC6A4A7935BD1E995UL;
  size_t hvalue = start;

#define TYPE_HASH(KIND, TYPE)                                                   \
  case Kind::KIND: {                                                            \
    n8bytes = std::hash<TYPE>()(main_.slice.read<TYPE>(rowOffset + colOffset)); \
    break;                                                                      \
  }

  std::for_each(std::begin(cols), std::end(cols),
                [&colProps, &hvalue, &rowOffset, this](const size_t index) {
                  const auto& item = colProps[index];
                  // is null
                  if (std::get<0>(item)) {
                    // std::__hash_combine()
                    hvalue = (hvalue ^ flip) >> 32;
                  } else {
                    // fetch value
                    size_t colOffset = std::get<1>(item);
                    Kind k = std::get<2>(item);
                    // we only support primitive types for keys
                    size_t n8bytes = 0;
                    switch (k) {
                      TYPE_HASH(BOOLEAN, bool)
                      TYPE_HASH(TINYINT, int8_t)
                      TYPE_HASH(SMALLINT, int16_t)
                      TYPE_HASH(INTEGER, int32_t)
                      TYPE_HASH(BIGINT, int64_t)
                      TYPE_HASH(REAL, int32_t)
                      TYPE_HASH(DOUBLE, int64_t)
                    case Kind::VARCHAR: {
                      // read 4 bytes offset and 4 bytes length
                      auto offset = main_.slice.read<int32_t>(rowOffset + colOffset);
                      auto len = main_.slice.read<int32_t>(rowOffset + colOffset + 4);

                      // read the real data from data_
                      // TODO(cao) - we don't need convert strings from bytes for hash
                      // instead, slice should be able to hash the range[offset, len] much cheaper
                      n8bytes = std::hash<std::string>()(data_.slice.read(offset, len));
                      break;
                    }
                    default:
                      throw NException("Only support primitive types as keys");
                    }

                    // update the hash with the item value's hash
                    hvalue = (hvalue ^ n8bytes) >> 32;
                  }
                });

#undef TYPE_HASH

  return hvalue;
}

// check if two rows are equal to each other on given columns
bool FlatBuffer::equal(size_t row1, size_t row2, const std::vector<size_t>& cols) const {
  auto row1Offset = std::get<0>(rows_[row1]);
  auto row2Offset = std::get<0>(rows_[row2]);
  FlatColumnProps colProps1 = columnProps(row1Offset);
  FlatColumnProps colProps2 = columnProps(row2Offset);

#define TYPE_COMPARE(KIND, TYPE)                                                                              \
  case Kind::KIND: {                                                                                          \
    if (main_.slice.read<TYPE>(row1Offset + colOffset1) != main_.slice.read<TYPE>(row2Offset + colOffset2)) { \
      return false;                                                                                           \
    }                                                                                                         \
    break;                                                                                                    \
  }

  for (auto index : cols) {
    const auto& item1 = colProps1[index];
    const auto& item2 = colProps2[index];
    // is null
    auto isItNull = std::get<0>(item1);
    if (isItNull != std::get<0>(item2)) {
      return false;
    }

    if (isItNull) {
      continue;
    }

    // fetch value
    size_t colOffset1 = std::get<1>(item1);
    size_t colOffset2 = std::get<1>(item2);
    Kind k = std::get<2>(item1);
    // we only support primitive types for keys
    switch (k) {
      TYPE_COMPARE(BOOLEAN, bool)
      TYPE_COMPARE(TINYINT, int8_t)
      TYPE_COMPARE(SMALLINT, int16_t)
      TYPE_COMPARE(INTEGER, int32_t)
      TYPE_COMPARE(BIGINT, int64_t)
      TYPE_COMPARE(REAL, int32_t)
      TYPE_COMPARE(DOUBLE, int64_t)
    case Kind::VARCHAR: {
      // read the real data from data_
      // TODO(cao) - we don't need convert strings from bytes for hash
      // instead, slice should be able to compare two byte range
      auto s1 = data_.slice.read(
        main_.slice.read<int32_t>(row1Offset + colOffset1),
        main_.slice.read<int32_t>(row1Offset + colOffset1 + 4));

      auto s2 = data_.slice.read(
        main_.slice.read<int32_t>(row2Offset + colOffset2),
        main_.slice.read<int32_t>(row2Offset + colOffset2 + 4));

      if (s1 != s2) {
        return false;
      }

      break;
    }
    default:
      throw NException("Only support primitive types as keys");
    }
  }

#undef TYPE_COMPARE

  return true;
}

// copy data of row1 into row2
bool FlatBuffer::copy(size_t row1, size_t row2, const UpdateCallback& callback, const std::vector<size_t>& cols) {
  // LOG(INFO) << "copy row " << row1 << " into " << row2;
  auto row1Offset = std::get<0>(rows_[row1]);
  auto row2Offset = std::get<0>(rows_[row2]);
  FlatColumnProps colProps1 = columnProps(row1Offset);
  FlatColumnProps colProps2 = columnProps(row2Offset);

#define UPDATE_COLUMN(COLUMN, KIND, TYPE)                       \
  case Kind::KIND: {                                            \
    auto targetOffset = row2Offset + colOffset2;                \
    auto nv = main_.slice.read<TYPE>(row1Offset + colOffset1);  \
    auto ov = main_.slice.read<TYPE>(targetOffset);             \
    TYPE x;                                                     \
    auto feedback = callback(COLUMN, Kind::KIND, &ov, &nv, &x); \
    N_ENSURE(feedback, "callback should always return true");   \
    main_.slice.write<TYPE>(targetOffset, x);                   \
    break;                                                      \
  }

  for (size_t i = 0, size = schema_->size(); i < size; ++i) {
    // skip keys
    if (std::any_of(cols.begin(), cols.end(), [i](size_t item) { return item == i; })) {
      continue;
    }

    const auto& item1 = colProps1[i];
    const auto& item2 = colProps2[i];

    // is null
    if (std::get<0>(item1)) {
      // TODO(cao) - ensure column i of row2 to be null;
      continue;
    }

    // fetch value
    size_t colOffset1 = std::get<1>(item1);
    size_t colOffset2 = std::get<1>(item2);
    Kind k = std::get<2>(item1);

    // we only support primitive types aggregation fields
    switch (k) {
      UPDATE_COLUMN(i, BOOLEAN, bool)
      UPDATE_COLUMN(i, TINYINT, int8_t)
      UPDATE_COLUMN(i, SMALLINT, int16_t)
      UPDATE_COLUMN(i, INTEGER, int32_t)
      UPDATE_COLUMN(i, BIGINT, int64_t)
      UPDATE_COLUMN(i, REAL, int32_t)
      UPDATE_COLUMN(i, DOUBLE, int64_t)
    default:
      throw NException("Only support primitive types as keys");
    }
  }

#undef TYPE_COMPARE

  return true;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
RowAccessor::RowAccessor(const FlatBuffer& fb, size_t offset, FlatColumnProps colProps)
  : fb_{ fb }, offset_{ offset }, colProps_{ std::move(colProps) } {}

bool RowAccessor::isNull(IndexType index) const {
  // null position for given field
  return std::get<0>(colProps_[index]);
}

#define READ_FIELD(TYPE, FUNC)                              \
  TYPE RowAccessor::FUNC(IndexType index) const {           \
    auto colOffset = std::get<1>(colProps_[index]);         \
    return fb_.main_.slice.read<TYPE>(offset_ + colOffset); \
  }

READ_FIELD(bool, readBool)
READ_FIELD(int8_t, readByte)
READ_FIELD(int16_t, readShort)
READ_FIELD(int32_t, readInt)
READ_FIELD(int64_t, readLong)
READ_FIELD(float, readFloat)
READ_FIELD(double, readDouble)

#undef READ_FIELD

std::string RowAccessor::readString(IndexType index) const {
  auto colOffset = std::get<1>(colProps_[index]);
  // read 4 bytes offset and 4 bytes length
  auto offset = fb_.main_.slice.read<int32_t>(offset_ + colOffset);
  auto len = fb_.main_.slice.read<int32_t>(offset_ + colOffset + 4);

  // read the real data from data_
  return fb_.data_.slice.read(offset, len);
}

// compound types
std::unique_ptr<nebula::surface::ListData> RowAccessor::readList(IndexType index) const {
  auto colOffset = std::get<1>(colProps_[index]);
  // read 4 bytes offset and 4 bytes length
  auto items = fb_.main_.slice.read<int32_t>(offset_ + colOffset);
  auto offset = fb_.main_.slice.read<int32_t>(offset_ + colOffset + 4);

  // can we cache this query or cache listAccessor?
  auto listType = std::dynamic_pointer_cast<nebula::type::ListType>(fb_.schema_->childType(index));

  // read the real data from data_
  return std::make_unique<ListAccessor>(items, offset, fb_.list_, fb_.data_, fb_.widthInMain(listType->childType(0)->k()));
}

std::unique_ptr<nebula::surface::MapData> RowAccessor::readMap(IndexType) const {
  throw NException("Map is not supported yet");
}
std::unique_ptr<nebula::surface::MapData> RowAccessor::readMap(const std::string&) const {
  throw NException("Map is not supported yet");
}

#define FORWARD_NAME_2_INDEX(TYPE, FUNC)                   \
  TYPE RowAccessor::FUNC(const std::string& field) const { \
    return FUNC(fb_.fields_.at(field));                    \
  }

FORWARD_NAME_2_INDEX(bool, isNull)
FORWARD_NAME_2_INDEX(bool, readBool)
FORWARD_NAME_2_INDEX(int8_t, readByte)
FORWARD_NAME_2_INDEX(int16_t, readShort)
FORWARD_NAME_2_INDEX(int32_t, readInt)
FORWARD_NAME_2_INDEX(int64_t, readLong)
FORWARD_NAME_2_INDEX(float, readFloat)
FORWARD_NAME_2_INDEX(double, readDouble)
FORWARD_NAME_2_INDEX(std::string, readString)
FORWARD_NAME_2_INDEX(std::unique_ptr<nebula::surface::ListData>, readList)

#undef FORWARD_NAME_2_INDEX

bool ListAccessor::isNull(IndexType index) const {
  return isOffsetNull(itemOffsets_[index]);
}

#define READ_FIELD(TYPE, FUNC)                                          \
  TYPE ListAccessor::FUNC(IndexType index) const {                      \
    return buffer_.slice.read<TYPE>(offset_ + itemOffsets_[index] + 1); \
  }

READ_FIELD(bool, readBool)
READ_FIELD(int8_t, readByte)
READ_FIELD(int16_t, readShort)
READ_FIELD(int32_t, readInt)
READ_FIELD(int64_t, readLong)
READ_FIELD(float, readFloat)
READ_FIELD(double, readDouble)

#undef READ_FIELD

std::string ListAccessor::readString(IndexType index) const {
  // we need to plus 1 to skip the first byte of null indicator
  const auto itemOffset = itemOffsets_[index] + 1;
  // read 4 bytes offset and 4 bytes length
  auto offset = buffer_.slice.read<int32_t>(offset_ + itemOffset);
  auto len = buffer_.slice.read<int32_t>(offset_ + itemOffset + 4);

  // read the real data from data_
  return strings_.slice.read(offset, len);
}

} // namespace keyed
} // namespace memory
} // namespace nebula