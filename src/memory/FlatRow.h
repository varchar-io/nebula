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

#pragma once

#include "common/Hash.h"
#include "common/Memory.h"
#include "type/Type.h"

#include "surface/DataSurface.h"

// DEFINE_int32(SLICE_SIZE, 64 * 1024, "slice size in bytes");

/**
 * Define a key-value style storage on a flat memory chunk. 
 * Supporting limited data type read and write without schema constraints.
 * 
 * Every key will get its offset when writing, and read will be random access by offset.
 * Supporting null value indicator in the storage. 
 * 
 * Reset will reset the writing cursor to beginning and wipe out all meta data.
 * Metadata is <key, offset>
 * 
 * Every value has size prefix, 1byte flag indicating if its null or not
 * Compound types are struct, map and list
 * PRIMITIVES
 * - 1byte flag size + {0, 1,2,4,8} bytes value for fixed (0 if it's null)
 * - 1byte flag + 4bytes size + {0, N} bytes for variable length value.
 * <p>
 * Struct
 * 1byte flag + {4 bytes size if flag=0} + repeat {field}
 * MAP
 * 1byte flag + {4 bytes items if flag=0} + {4 bytes size if not null and having items} + repeat {key, value}
 * List
 * 1byte flag + {4 bytes items if flag=0} + {4 bytes size if not null and having items} + repeat {item}
 */
namespace nebula {
namespace memory {

using nebula::common::ExtendableSlice;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::Tree;
using nebula::type::TreeBase;
using nebula::type::TreeNode;
using nebula::type::TypeNode;

/**
 * A flat memory serialization format for a ROW structure.
 * internally it's just a byte array. The data layout like this
 * it's designed to be reused to carry every row data from reader to writer
 * it's optimized for fast access but not optimized for storage size -
 * so row data memory may be 2x larger than original row data
 * <p>
 */

class FlatRow : public nebula::surface::RowData {
public:
  static constexpr NByte NULL_BYTE = 0;
  static constexpr NByte STRING_FLAG = 127;
  static constexpr int16_t LIST_FLAG = 99 << 8;

  FlatRow(size_t initSliceSize, bool nullIfMissing = false)
    : slice_{ initSliceSize }, nullIfMissing_{ nullIfMissing }, cursor_{ 0 } {}
  virtual ~FlatRow() = default;

  // initialize states for writing a new row
  void reset() {
    cursor_ = 0;
    meta_.clear();
  }

  void writeNull(const std::string& key) {
    // get writing position
    auto pos = moveKey(key, 1);
    slice_.write(pos, NULL_BYTE);
  }

  // write data at given memory offset for specified node
  // or using is_arithmetic to limit to types in bool, byte, short, int, long, float, double
  template <typename T>
  auto write(const std::string& key, const T& value)
    -> typename std::enable_if<std::is_scalar<T>::value, size_t>::type {
    constexpr NByte width = sizeof(T);
    // flag of width + value
    auto pos = moveKey(key, width + 1);
    slice_.write(pos, width);
    slice_.write(pos + 1, value);

    return width;
  }

  size_t write(const std::string& key, const char* str, size_t length) {
    // flag of string [STRING_FLAG][LENGTH][bytes]
    auto size = 1 + 4 + length;
    auto pos = moveKey(key, size);
    slice_.write(pos++, STRING_FLAG);
    slice_.write(pos, (uint32_t)length);
    pos += 4;

    // write string bytes out
    slice_.write(pos, str, length);
    return size;
  }

  // write spceial value as string
  size_t write(const std::string& key, const std::string& str) {
    return write(key, str.data(), str.size());
  }

  // support array/vector of scalar types including string data support as well
  template <typename T>
  auto write(const std::string& key, const std::vector<T>& value)
    -> typename std::enable_if<std::is_scalar<T>::value, size_t>::type {
    constexpr NByte width = sizeof(T);
    auto length = value.size();
    // format: [LIST][width][length][data]
    auto size = 2 + 4 + (width * length);
    auto pos = moveKey(key, size);
    slice_.write(pos, LIST_FLAG | width);
    pos += 2;
    slice_.write(pos, (uint32_t)length);
    pos += 4;

    // write each item
    for (const T& item : value) {
      slice_.write(pos, item);
      pos += width;
    }

    return size;
  }

  // support array/vector of scalar types including string data support as well
  size_t write(const std::string& key, const std::vector<std::string>& value) {
    auto length = value.size();
    // format: [LIST][STRING][length][data]
    auto start = cursor_;

    // write header
    auto pos = moveKey(key, 6);
    slice_.write(pos, LIST_FLAG | STRING_FLAG);
    pos += 2;
    slice_.write(pos, (uint32_t)length);
    pos += 4;

    // write each string in the list by writing offset first
    // format [length vector][data bytes]
    // example
    // first (length+1) * 4 bytes to hold all offsets of all strings
    // each item length equals (next offset - current offset)
    cursor_ = pos + (length + 1) * 4;
    for (const std::string& str : value) {
      // write each string offset in data section
      auto len = str.size();
      slice_.write(pos, (int32_t)cursor_);
      pos += 4;

      // write the string in target cursor
      slice_.write(cursor_, str.data(), len);
      cursor_ += len;
    }

    // last item offset
    slice_.write(pos, (int32_t)cursor_);

    return cursor_ - start;
  }

  inline bool hasKey(const std::string& key) const noexcept {
    return meta_.find(key) != meta_.end();
  }

public:
#define INTERFACE_IMPL(RT, NAME) \
  virtual RT NAME(const std::string&) const override;

  INTERFACE_IMPL(bool, isNull)
  INTERFACE_IMPL(bool, readBool)
  INTERFACE_IMPL(int8_t, readByte)
  INTERFACE_IMPL(int16_t, readShort)
  INTERFACE_IMPL(int32_t, readInt)
  INTERFACE_IMPL(int64_t, readLong)
  INTERFACE_IMPL(float, readFloat)
  INTERFACE_IMPL(double, readDouble)
  INTERFACE_IMPL(int128_t, readInt128)
  INTERFACE_IMPL(std::string_view, readString)
  INTERFACE_IMPL(std::unique_ptr<nebula::surface::ListData>, readList)
  INTERFACE_IMPL(std::unique_ptr<nebula::surface::MapData>, readMap)

#undef INTERFACE_IMPL

private:
  inline size_t moveKey(const std::string& key, size_t size) {
    N_ENSURE(meta_.find(key) == meta_.end(), "do not overwrite key");
    // record key offset
    auto current = cursor_;
    meta_[key] = current;

    // move cursor
    cursor_ = current + size;
    return current;
  }

private:
  // data containers
  ExtendableSlice slice_;
  // a flag to help answering isNull question
  bool nullIfMissing_;

  // write states
  size_t cursor_;
  nebula::common::unordered_map<std::string, size_t> meta_;
};

class FlatList : public nebula::surface::ListData {
public:
  FlatList(size_t size, int8_t type, size_t base, const ExtendableSlice& slice)
    : ListData(size), type_{ type }, base_{ base }, slice_{ slice } {}
  virtual ~FlatList() = default;

// all interfaces - not support list of compounds types for now
#define LIST_INTERFACE(RT, NAME) \
  virtual RT NAME(size_t) const override;

  LIST_INTERFACE(bool, isNull)
  LIST_INTERFACE(bool, readBool)
  LIST_INTERFACE(int8_t, readByte)
  LIST_INTERFACE(int16_t, readShort)
  LIST_INTERFACE(int32_t, readInt)
  LIST_INTERFACE(int64_t, readLong)
  LIST_INTERFACE(float, readFloat)
  LIST_INTERFACE(double, readDouble)
  LIST_INTERFACE(int128_t, readInt128)
  LIST_INTERFACE(std::string_view, readString)

#undef LIST_INTERFACE

private:
  // width of target type or string flag
  int8_t type_;
  // base offset for current list in slice ref
  size_t base_;
  // reference to data store
  const ExtendableSlice& slice_;
};

} // namespace memory
} // namespace nebula