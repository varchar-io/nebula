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

#include <gflags/gflags.h>
DEFINE_uint64(FB_MAIN_PAGE, 1024 * 1024, "Main memory page size");
DEFINE_uint64(FB_DATA_PAGE, 4096 * 1024, "Data memory page size");
DEFINE_uint64(FB_LIST_PAGE, 2048 * 1024, "List memory page size");

namespace nebula {
namespace memory {
namespace keyed {

using nebula::common::ExtendableSlice;
using Range = nebula::common::PRange;
using nebula::surface::ListData;
using nebula::surface::RowData;
using nebula::type::Kind;
using nebula::type::ListType;
using nebula::type::TypeNode;

void FlatBuffer::initSchema() noexcept {
  // build name to index look up
  nm_.reserve(numColumns_);
  cops_.reserve(numColumns_);

  // initialize keys and values
  for (size_t i = 0; i < numColumns_; ++i) {
    // if current column is aggregate column
    auto ia = fields_.at(i)->isAggregate();
    auto f = schema_->childType(i);
    nm_.emplace(f->name(), i);
    auto kind = f->k();

    // get the column width
    auto width = widthInMain(kind);

    // for aggregated columns we reserve space even it's null
    if (ia) {
      width = std::max(width, MAX_ALIGNMENT);
    }

    // generate column parser for each column
    cops_.emplace_back(genParser(f, i, kind), ia ? genSketcher(i) : nullptr, kind, width);
  }
}

FlatBuffer::FlatBuffer(const nebula::type::Schema& schema,
                       const nebula::surface::eval::Fields& fields)
  : schema_{ schema },
    numColumns_{ schema->size() },
    fields_{ fields },
    chunk_{ nullptr },
    chunkSize_{ 0 },
    main_{ std::make_unique<Buffer>(FLAGS_FB_MAIN_PAGE) },
    data_{ std::make_unique<Buffer>(FLAGS_FB_DATA_PAGE) },
    list_{ std::make_unique<Buffer>(FLAGS_FB_LIST_PAGE) } {
  this->initSchema();
}

// initialize a read-only flat buffer with given serialized data
// NOTE: This read-only object doesn't own the data buffer neither copy, it only references it.
//       Hence external buffer holder needs to be live the same scope this object,
//       We can improve this interface later.
FlatBuffer::FlatBuffer(const nebula::type::Schema& schema,
                       const nebula::surface::eval::Fields& fields,
                       NByte* data)
  : schema_{ schema },
    numColumns_{ schema->size() },
    fields_{ fields },
    chunk_{ data },
    chunkSize_{ 0 } {
  // 1. initialize the column align property based on the meta blob
  this->initSchema();

  // deserialize data for rows and all data blocks
  // refer method serialize for reverse logic
  size_t offset = 0;
  const auto readSizeT = [&data, &offset]() {
    auto value = *reinterpret_cast<const size_t*>(data + offset);
    offset += SIZET_SIZE;
    return value;
  };

  auto numRows = readSizeT();
  if (numRows == 0) {
    return;
  }

  // this section will have numRows * SIZE_T for each row offset
  auto offsetOffset = offset;

  // skip it for now to prepare main data
  offset += numRows * SIZET_SIZE;

  // write main block size
  auto mainSize = readSizeT();
  auto dataSize = readSizeT();
  auto listSize = readSizeT();
  auto magic = readSizeT();
  N_ENSURE(magic == MAGIC, "magic mismatch: corrupted flat buffer data");

  // build up each buffer
  main_ = std::make_unique<Buffer>(mainSize, data + offset);
  offset += mainSize;

  data_ = std::make_unique<Buffer>(dataSize, data + offset);
  offset += dataSize;

  list_ = std::make_unique<Buffer>(listSize, data + offset);
  offset += listSize;

  // we know how big the chunk is now
  // used for information only, may not be exactly the same memory chunk size.
  chunkSize_ = offset;

  // populate all rows properties
  rows_.reserve(numRows);

  // reset back to read each offset value
  offset = offsetOffset;
  for (size_t i = 0; i < numRows; ++i) {
    // read each column props of all rows (should it be lazy?)
    auto rowOffset = readSizeT();
    rows_.emplace_back(rowOffset, rebuildColumnProps(rowOffset));
  }
}

// this defines how many bytes for each column take in main buffer
// for scalar type, they are fixed width data, so we have width for non-null values
// for binary type such as string, we store offset in related buffer and length as well
size_t FlatBuffer::widthInMain(Kind kind) noexcept {
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
    SCALAR_WIDTH_DISTR(INT128)
  case Kind::VARCHAR: {
    // 4 bytes offset + 4 bytes length
    return 8;
  }
  case Kind::ARRAY: {
    // 4 bytes number of items + 4 bytes offset in list
    return 8;
  }
  default:
    return 0;
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
size_t FlatBuffer::append(T value, Buffer& dest, size_t align) {
  size_t len = dest.slice.writeAlign(dest.offset, value, align);
  dest.offset += len;

  // return current offset
  return dest.offset;
}

template <>
size_t FlatBuffer::append(std::string_view str, Buffer& dest, size_t) {
  // write variable data into dirty buffer and get offset and length
  auto len = data_->slice.write(data_->offset, (NByte*)str.data(), str.size());

  // write offset and length in main chunk
  dest.offset += Range::write(dest.slice, dest.offset, data_->offset, len);

  // move forward data offset for next
  data_->offset += len;
  return len;
}

// TODO(cao) - right now, we're adding list at first level buffer
// which makes it's impossible to update list type in place.
// If we want to support updating list, we should move its data to next level (like string).
// We can considering using a dynamic chained buffers to allow multiple levels of dirty buffers.
size_t FlatBuffer::appendList(Kind itemKind, std::unique_ptr<ListData> list) {
  auto offset = list_->offset;
  auto count = list->getItems();

  // add every single item here, the function is similar to add method
  // which iterates over columns instead
  for (auto i = 0; i < count; ++i) {
    if (appendNull(list->isNull(i), itemKind, *list_)) {
      // add null for this field
      continue;
    }

#define SCALAR_DATA_DISTR(KIND, FUNC) \
  case Kind::KIND: {                  \
    append(list->FUNC(i), *list_);    \
    break;                            \
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
      SCALAR_DATA_DISTR(VARCHAR, readString)
      SCALAR_DATA_DISTR(INT128, readInt128)
    default:
      throw NException("other types not supported yet");
    }

#undef SCALAR_DATA_DISTR
  }

  return offset;
}

bool FlatBuffer::rollback() {
  // has last row to roll back
  if (rows_.size() > 0) {
    // remove last row
    rows_.pop_back();

    // every buffer reset offset
    main_->offset = std::get<0>(last_);
    data_->offset = std::get<1>(last_);
    list_->offset = std::get<2>(last_);

    return true;
  }

  return false;
}

std::function<void(const RowData&)> FlatBuffer::genParser(
  const TypeNode& tn, size_t i, nebula::type::Kind kind) noexcept {

#define SCALAR_DATA_DISTR(KIND, FUNC)                                                         \
  case Kind::KIND: {                                                                          \
    return [this, i](const RowData& row) { append(row.FUNC(i), *main_, cops_.at(i).width); }; \
  }

  // add solid values for each type
  switch (kind) {
    SCALAR_DATA_DISTR(BOOLEAN, readBool)
    SCALAR_DATA_DISTR(TINYINT, readByte)
    SCALAR_DATA_DISTR(SMALLINT, readShort)
    SCALAR_DATA_DISTR(INTEGER, readInt)
    SCALAR_DATA_DISTR(BIGINT, readLong)
    SCALAR_DATA_DISTR(REAL, readFloat)
    SCALAR_DATA_DISTR(DOUBLE, readDouble)
    SCALAR_DATA_DISTR(INT128, readInt128)
    SCALAR_DATA_DISTR(VARCHAR, readString)

  case Kind::ARRAY: {
    auto listType = std::static_pointer_cast<ListType>(tn);

    return [this, childKind = listType->childType(0)->k(), i](const RowData& row) {
      auto list = row.readList(i);

      // write 4 bytes of N = number of items
      auto listItems = list->getItems();
      auto listOffset = appendList(childKind, std::move(list));
      main_->offset += Range::write(main_->slice, main_->offset, listItems, listOffset);
    };
  }
  default:
    return [i](const RowData&) {
      LOG(INFO) << "Parse on un-supported column: " << i;
    };
  }

#undef SCALAR_DATA_DISTR
}

Sketcher FlatBuffer::genSketcher(size_t i) noexcept {
  const auto& f = fields_.at(i);
  const auto ot = f->outputType();
  const auto it = f->inputType();
  const auto isAggregate = f->isAggregate();
  if (!isAggregate) {
    return {};
  }

// below provides a template to write core logic for all input / output type combinations
#define LOGIC_BY_IO(O, I)                   \
  case Kind::I: {                           \
    return [this, i]() -> auto {            \
      const auto& f = fields_.at(i);        \
      return f->sketch<Kind::O, Kind::I>(); \
    };                                      \
    break;                                  \
  }

  ITERATE_BY_IO(ot, it)

#undef LOGIC_BY_IO

  // no suitable combination found
  return {};
}

// add a row into current batch
// if cols is not empty, we only populate those fileds, otherwise, populate all
size_t FlatBuffer::add(const nebula::surface::RowData& row) {
  // record current state before adding a new row - used for rollback
  last_ = std::make_tuple(main_->offset, data_->offset, list_->offset);

  // current row offset
  const auto rowOffset = main_->offset;

  // in the main memory, we're push all nulls for first X bytes (x = numColumns)
  // This is designed for nulls fast load of memory locality
  // this is why we have two loops
  FlatColumnProps columnProps;
  columnProps.reserve(numColumns_);
  std::vector<bool> nulls;
  nulls.reserve(numColumns_);
  for (size_t i = 0; i < numColumns_; ++i) {
    auto nv = appendNull(row.isNull(i), cops_.at(i).kind, *main_);
    nulls.push_back(nv);
  }

  // write data of each column through its parser
  for (size_t i = 0; i < numColumns_; ++i) {
    // push each column props
    auto nv = nulls.at(i);
    const auto& cop = cops_.at(i);
    auto ia = cop.isAggregate();
    auto sketch = ia ? row.getAggregator(i) : nullptr;
    columnProps.emplace_back(nv, main_->offset - rowOffset, sketch);
    if (!nv) {
      cop.parser(row);
    } else if (ia) {
      // reserve space for aligned column
      main_->offset += main_->slice.writeAlign(main_->offset, 0, cop.width);
    }
  }

  // after processing all columns, we got the row offset and length, record it here
  rows_.emplace_back(rowOffset, std::move(columnProps));

  return rowOffset;
}

// this method is used to pair partial add (when cols set is not empty)
// scenario: when a new added row will need to be a new entry, we fullfil all columns rather than keys.
// this fullfilment is only for last row, so we have to have sanity check
size_t FlatBuffer::resume(const nebula::surface::RowData& row,
                          const std::unordered_set<size_t>& cols,
                          const size_t rowId) {
  N_ENSURE_EQ(rowId + 1, rows_.size(), "must be the last row");

  auto origin = main_->offset;
  // get row properties for this existing row
  auto& rowProps = rows_.at(rowId);
  auto& colPropsList = rows_.at(rowId).colProps;
  auto rowOffset = rowProps.offset;
  for (size_t i : cols) {
    // if the column is null, we keep what it is
    // otherwise we fill the value and update its column props
    auto& colProps = colPropsList.at(i);
    colProps.offset = main_->offset - rowOffset;
    if (!colProps.isNull) {
      cops_.at(i).parser(row);
    }
  }

  // return the delta size of the resumed values
  return main_->offset - origin;
}

// random access to a row - may require internal seek
const std::unique_ptr<RowData> FlatBuffer::crow(size_t rowId) const {
  // pass in row offset and column props of this row
  return std::make_unique<RowAccessor>(*this, rows_.at(rowId));
}

const RowData& FlatBuffer::row(size_t rowId) {
  // pass in row offset and column props of this row
  current_ = std::make_unique<RowAccessor>(*this, rows_.at(rowId));
  return *current_;
}

FlatColumnProps FlatBuffer::rebuildColumnProps(size_t rowOffset) {
  // do not use constructor with count instances of T
  FlatColumnProps colProps;
  colProps.reserve(numColumns_);

  // first numCols bytes store nulls for all columns
  auto colOffset = numColumns_;
  for (size_t i = 0; i < numColumns_; ++i) {
    auto nullByte = main_->slice.read<int8_t>(rowOffset + i);
    bool nv = (nullByte & HIGH6_1) != 0;
    const auto& cop = cops_.at(i);

    // build column properties without sketch
    auto& cp = colProps.emplace_back(nv, colOffset);
    if (!nv) {
      colOffset += cop.width;
    }

    // for aggregated fields, rebuild its sketch from the serialized binary
    if (cop.isAggregate()) {
      cp.sketch = cop.sketcher();
      N_ENSURE_NOT_NULL(cp.sketch, "aggregated field should have sketch");
      // load data of the sketch from main buffer since it's fit
      auto offset = rowOffset + cp.offset;
      if (cp.sketch->fit(cop.width)) {
        auto size = cp.sketch->load(main_->slice, offset);
        N_ENSURE(size <= cop.width, "sketch guranteed data size smaller than alignment");
      } else {
        auto r = Range::make(main_->slice, offset);
        auto size = cp.sketch->load(data_->slice, r.offset);
        N_ENSURE(size == r.size, "loaded size should be the same as it stored");
      }
    }
  }

  return colProps;
}

// serialize sketches into data section and mark its offset/length at field
// if the sketch serialized data is fixed within aignment, we can use it directly
size_t FlatBuffer::serializeSketches() const {
  // data size incremented for sketch serialization
  auto size = 0;
  for (auto& cp : rows_) {
    for (size_t i = 0; i < numColumns_; ++i) {
      // for (auto& p : cp.colProps) {
      auto& p = cp.colProps.at(i);
      // if we have sketch to serialize out
      if (p.sketch) {
        auto offset = (cp.offset + p.offset);
        const auto& cop = cops_.at(i);
        // if fit within main
        if (p.sketch->fit(cop.width)) {
          N_ENSURE(p.sketch->serialize(main_->slice, offset) <= cop.width,
                   "serialzied size should not out of space");
        } else {
          auto len = p.sketch->serialize(data_->slice, data_->offset);
          // record the data offset and length for this binary in main
          Range::write(main_->slice, offset, data_->offset, len);
          // grow data size
          data_->offset += len;

          // only counted the data size increase - not counting if fit in main
          size += len;
        }
      }
    }
  }

  return size;
}

size_t FlatBuffer::serialize(NByte* buffer) const {
  auto offset = 0;
  // write size_t value
  const auto writeSizeT = [&buffer, &offset](size_t value) {
    *reinterpret_cast<size_t*>(buffer + offset) = value;
    offset += SIZET_SIZE;
  };

  // write num rows always in case reader size check row size
  // rather than binary size
  const auto numRows = rows_.size();
  writeSizeT(numRows);
  if (numRows == 0) {
    return 0;
  }

  // write all rows' offset
  for (size_t i = 0; i < numRows; ++i) {
    writeSizeT(rows_.at(i).offset);
  }

  // write main block size
  writeSizeT(main_->offset);

  // write data block size
  writeSizeT(data_->offset);

  // write list block size
  writeSizeT(list_->offset);

  // write a reserved value
  writeSizeT(MAGIC);

  // write all main bits
  offset += main_->slice.copy(buffer, offset, main_->offset);

  // write all data bits
  offset += data_->slice.copy(buffer, offset, data_->offset);

  // write all list bits
  offset += list_->slice.copy(buffer, offset, list_->offset);

  // total size of the serialized data
  return offset;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
RowAccessor::RowAccessor(const FlatBuffer& fb, const RowProps& rowProps)
  : fb_{ fb }, rowProps_{ rowProps } {}

bool RowAccessor::isNull(IndexType index) const {
  // null position for given field
  return rowProps_.colProps.at(index).isNull;
}

#define READ_FIELD(TYPE, FUNC)                                                               \
  TYPE RowAccessor::FUNC(IndexType index) const {                                            \
    return fb_.main_->slice.read<TYPE>(rowProps_.offset + rowProps_.colProps[index].offset); \
  }

READ_FIELD(bool, readBool)
READ_FIELD(int8_t, readByte)
READ_FIELD(int16_t, readShort)
READ_FIELD(int32_t, readInt)
READ_FIELD(int64_t, readLong)
READ_FIELD(float, readFloat)
READ_FIELD(double, readDouble)
READ_FIELD(int128_t, readInt128)

#undef READ_FIELD

std::string_view RowAccessor::readString(IndexType index) const {
  auto colOffset = rowProps_.colProps[index].offset;
  return fb_.read(rowProps_.offset, colOffset);
}

// compound types
std::unique_ptr<nebula::surface::ListData> RowAccessor::readList(IndexType index) const {
  auto colOffset = rowProps_.colProps[index].offset;
  // read 4 bytes offset and 4 bytes length
  auto lo = rowProps_.offset + colOffset;
  auto r = Range::make(fb_.main_->slice, lo);

  // can we cache this query or cache listAccessor?
  auto listType = std::dynamic_pointer_cast<nebula::type::ListType>(fb_.schema_->childType(index));

  // read the real data from data_
  return std::make_unique<ListAccessor>(
    r.offset, r.size, *fb_.list_, *fb_.data_, fb_.widthInMain(listType->childType(0)->k()));
}

std::unique_ptr<nebula::surface::MapData> RowAccessor::readMap(IndexType) const {
  throw NException("Map is not supported yet");
}
std::unique_ptr<nebula::surface::MapData> RowAccessor::readMap(const std::string&) const {
  throw NException("Map is not supported yet");
}

inline std::shared_ptr<nebula::surface::eval::Sketch> RowAccessor::getAggregator(IndexType index) const {
  return rowProps_.colProps[index].sketch;
}

#define FORWARD_NAME_2_INDEX(TYPE, FUNC)                   \
  TYPE RowAccessor::FUNC(const std::string& field) const { \
    return FUNC(fb_.nm_.at(field));                        \
  }

FORWARD_NAME_2_INDEX(bool, isNull)
FORWARD_NAME_2_INDEX(bool, readBool)
FORWARD_NAME_2_INDEX(int8_t, readByte)
FORWARD_NAME_2_INDEX(int16_t, readShort)
FORWARD_NAME_2_INDEX(int32_t, readInt)
FORWARD_NAME_2_INDEX(int64_t, readLong)
FORWARD_NAME_2_INDEX(float, readFloat)
FORWARD_NAME_2_INDEX(double, readDouble)
FORWARD_NAME_2_INDEX(int128_t, readInt128)
FORWARD_NAME_2_INDEX(std::string_view, readString)
FORWARD_NAME_2_INDEX(std::unique_ptr<nebula::surface::ListData>, readList)
FORWARD_NAME_2_INDEX(std::shared_ptr<nebula::surface::eval::Sketch>, getAggregator)

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
READ_FIELD(int128_t, readInt128)

#undef READ_FIELD

std::string_view ListAccessor::readString(IndexType index) const {
  // we need to plus 1 to skip the first byte of null indicator
  const auto itemOffset = itemOffsets_[index] + 1;
  // read 4 bytes offset and 4 bytes length
  auto r = Range::make(buffer_.slice, offset_ + itemOffset);

  // read the real data from data_
  return strings_.slice.read(r.offset, r.size);
}

} // namespace keyed
} // namespace memory
} // namespace nebula