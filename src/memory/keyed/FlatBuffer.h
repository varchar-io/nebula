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

#include <stack>
#include <unordered_map>
#include "common/Memory.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

// DEFINE_int32(SLICE_SIZE, 64 * 1024, "slice size in bytes");

/**
 * Build a flat buffer that can be used in hash table to build keys and update directly.
 * 
 * Every value has 1byte = HIGH4 (NULL) + LOW4 (TYPE).
 * Scalar types are embeded and others are referenced by offset and length in buffer.
 * So we have two memory chunks. 
 * 
 * In main memory chunk, it starts with # of bytes for nulls
 * bool/INT/float types: width bytes. 
 * string type: 8 bytes [4 bytes offset, 4 bytes length]
 * list type: 8 bytes [4 bytes of value N (number of items), 4 bytes offset in list buffer]
 * String data stored in data_
 * List items stores at list_
 * So In main_, we know exactly size of each type if it has non-null value
 * map type: not support for now 
 * struct type: not support for now
 */
namespace nebula {
namespace memory {
namespace keyed {

static constexpr int8_t HIGH6_1 = 1 << 6;

class RowAccessor;

struct Buffer {
  Buffer(size_t page) : offset{ 0 }, slice{ page } {}
  size_t offset;
  nebula::common::PagedSlice slice;
};

// define column properties of given row: nullable, column offset in main, kind.
using FlatColumnProps = std::vector<std::tuple<bool, size_t, nebula::type::Kind>>;

// an update callback signature
using UpdateCallback = std::function<bool(size_t, nebula::type::Kind, void*, void*, void*)>;

class FlatBuffer {
  // offset and length
  using OL = std::tuple<size_t, size_t>;

public:
  FlatBuffer(const nebula::type::Schema& schema);

  virtual ~FlatBuffer() = default;

  // add a row into current batch
  size_t add(const nebula::surface::RowData& row);

  // this method only rollback last added row and the only one row only.
  size_t rollback();

  // random access to a row - may require internal seek
  const nebula::surface::RowData& row(size_t);

  // compute hash value of given row and column list
  size_t hash(size_t rowId, const std::vector<size_t>& cols) const;

  // check if two rows are equal to each other on given columns
  bool equal(size_t row1, size_t row2, const std::vector<size_t>& cols) const;

  // copy data of row1 into row2
  bool copy(size_t row1, size_t row2, const UpdateCallback& callback, const std::vector<size_t>& cols);

  inline auto getRows() const {
    return rows_.size();
  }

private:
  bool appendNull(bool, nebula::type::Kind, Buffer&);

  template <typename T>
  size_t appendScalar(T, Buffer&);

  // goes to data_
  size_t appendString(const std::string&, Buffer&);

  // goes to list_
  size_t appendList(nebula::type::Kind, std::unique_ptr<nebula::surface::ListData>);

  // get column properties of given row
  FlatColumnProps columnProps(size_t, size_t) const;

  static size_t widthInMain(nebula::type::Kind);

private:
  // record each row's offset and length
  std::tuple<size_t, size_t, size_t> last_;
  std::vector<OL> rows_;
  nebula::type::Schema schema_;
  Buffer main_;
  Buffer data_;
  Buffer list_;

  // A row accessor cursor to read data of given row
  friend class RowAccessor;
  std::unique_ptr<RowAccessor> current_;

  // fields_ is name->index mapping
  std::unordered_map<std::string, size_t> fields_;
  // widths_ indicats the column's width in main_ buffer if not null.
  std::vector<size_t> widths_;
};

class RowAccessor : public nebula::surface::RowData {
public:
  RowAccessor(FlatBuffer&, size_t, FlatColumnProps);
  virtual ~RowAccessor() = default;

public:
  bool isNull(const std::string& field) const override;
  bool readBool(const std::string& field) const override;
  int8_t readByte(const std::string& field) const override;
  int16_t readShort(const std::string& field) const override;
  int32_t readInt(const std::string& field) const override;
  int64_t readLong(const std::string& field) const override;
  float readFloat(const std::string& field) const override;
  double readDouble(const std::string& field) const override;
  std::string readString(const std::string& field) const override;

  // compound types
  std::unique_ptr<nebula::surface::ListData> readList(const std::string& field) const override;
  std::unique_ptr<nebula::surface::MapData> readMap(const std::string& field) const override;

private:
  FlatBuffer& fb_;

  // current row offset and all column properties
  size_t offset_;
  FlatColumnProps colProps_;
};

using nebula::surface::IndexType;
class ListAccessor : public nebula::surface::ListData {
public:
  ListAccessor(IndexType items, IndexType offset, Buffer& buffer, Buffer& strings, size_t itemWidth)
    : ListData(items),
      offset_{ offset },
      buffer_{ buffer },
      strings_{ strings },
      itemWidth_{ itemWidth } {

    // move current index to index and adjust current offset
    itemOffsets_.reserve(items);
    size_t itemOffset = 0;
    for (auto i = 0; i < items; ++i) {
      itemOffsets_.push_back(itemOffset);
      // check if its null
      if (!isOffsetNull(itemOffset)) {
        itemOffset += itemWidth_;
      }

      // the always null byte
      itemOffset += 1;
    }
  }

  bool isNull(IndexType index) const override;
  bool readBool(IndexType index) const override;
  std::int8_t readByte(IndexType index) const override;
  int16_t readShort(IndexType index) const override;
  int32_t readInt(IndexType index) const override;
  int64_t readLong(IndexType index) const override;
  float readFloat(IndexType index) const override;
  double readDouble(IndexType index) const override;
  std::string readString(IndexType index) const override;

private:
  inline bool isOffsetNull(size_t itemOffset) const {
    return (HIGH6_1 & (buffer_.slice.read<int8_t>(offset_ + itemOffset))) != 0;
  }

private:
  IndexType offset_;
  Buffer& buffer_;
  Buffer& strings_;
  size_t itemWidth_;

  std::vector<size_t> itemOffsets_;
};

} // namespace keyed
} // namespace memory
} // namespace nebula