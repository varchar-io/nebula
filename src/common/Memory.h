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

#include <folly/compression/Compression.h>
#include <glog/logging.h>
#include <iostream>
#include <numeric>

#include "Errors.h"
#include "Hash.h"
#include "Int128.h"
#include "Likely.h"

// TODO(cao): import jemalloc for global new/delete allocation
// void* operator new(size_t size) {
//   void* p = std::malloc(size);
//   // std::cout << "Called Nebula New Operator: " << size << " bytes" << "\n";
//   return p;
// }
// void operator delete(void* p) noexcept {
//   // std::cout << "Called Nebula Delete Operator. \n";
//   std::free(p);
// }

// define Nebula byte type
#ifndef NByte
#define NByte int8_t
#endif

namespace nebula {
namespace common {

class Pool {
public:
  virtual ~Pool() = default;

  inline void* allocate(size_t size) {
    return std::malloc(size);
  }

  inline void free(void* p) {
    std::free(p);
  }

  void* extend(void* p, size_t size, size_t newSize) {
    N_ENSURE_GT(newSize, size, "new size should be larger than original size");

    // extend the memory if possible
    void* newP = std::realloc(p, newSize);
    if (UNLIKELY(!newP)) {
      free(p);
      throw std::bad_alloc();
    }

    return newP;
  }

  static Pool& getDefault();

private:
  // TODO(cao): a start point of pool impl, need a better pool management
  Pool() = default;
};

/**
 * A slice represents a N sized memory chunk.
 * By default, a 64K page is provided. 
 */
class Slice {
public:
  virtual ~Slice() {
    if (ownbuffer_) {
      if (!!ptr_) {
        pool_.free(static_cast<void*>(ptr_));
      } else {
        LOG(ERROR) << "A slice should hold a valid pointer";
      }
    }
  }

  inline size_t size() const {
    return size_;
  }

  inline NByte* ptr() const {
    return ptr_;
  }

protected:
  // A read-only slice!! wrapping an external buffer but not owning it
  Slice(const NByte* buffer, size_t size, bool own = false)
    : pool_{ Pool::getDefault() }, size_{ size }, ptr_{ const_cast<NByte*>(buffer) }, ownbuffer_{ own } {}
  Slice(size_t size) : pool_{ Pool::getDefault() }, size_{ size }, ptr_{ static_cast<NByte*>(pool_.allocate(size)) }, ownbuffer_{ true } {}
  Slice(Slice&) = delete;
  Slice(Slice&&) = delete;
  Slice& operator=(Slice&) = delete;
  Slice& operator=(Slice&&) = delete;

  // memory pool implementation
  Pool& pool_;

  // size of current buffer
  size_t size_;

  // memory pointer
  NByte* ptr_;

  // own the buffer?
  bool ownbuffer_;
};

class OneSlice : public Slice {
public:
  OneSlice(size_t size) : Slice{ size } {}
  ~OneSlice() = default;
};

/**
 * A paged slice is a chain of slices with given sized slice of chunks.
 * A paged slice is a slice, and it can have more slices as extensions when necessary.
 */
class PagedSlice : public Slice {
public:
  PagedSlice(const NByte* buffer, size_t size) : Slice{ buffer, size }, slices_{ 1 }, numExtended_{ 0 } {}
  PagedSlice(size_t page) : Slice{ page }, slices_{ 1 }, numExtended_{ 0 } {}
  ~PagedSlice() = default;

  // append a bytes array of length bytes to position
  inline size_t write(size_t position, const char* data, size_t length) {
    return write(position, (NByte*)data, length);
  }

  size_t write(size_t position, const NByte* data, size_t length);

  // append a data object at position using sizeof to determine its size
  template <typename T>
  auto write(size_t position, const T& value) -> typename std::enable_if<std::is_scalar<T>::value, size_t>::type {
    // write a scalar typed data in a given alignment space
    // auto size = std::max(sizeof(T), alignment);
    constexpr size_t size = sizeof(T);
    return writeSize(position, value, size);
  }

  template <typename T>
  size_t writeAlign(size_t position, const T& value, size_t alignment) {
    // write a scalar typed data in a given alignment space
    // auto size = std::max(sizeof(T), alignment);
    constexpr size_t size = sizeof(T);
    return writeSize(position, value, std::max(size, alignment));
  }

  // NOTE: (found a g++ bug)
  // It declares the method is not mark as const if we change the signature as
  // auto read(size_t position) -> typename std::enable_if<std::is_scalar<T>::value, T&>::type const {
  template <typename T>
  typename std::enable_if<std::is_scalar<T>::value, T>::type read(size_t position) const {
    constexpr size_t size = sizeof(T);
    N_ENSURE(position + size <= capacity(), "invalid position to read");

    return *reinterpret_cast<T*>(this->ptr_ + position);
  }

  std::string_view read(size_t position, size_t length) const {
    N_ENSURE(position + length <= capacity(), "invalid position read string");

    // build data using copy elision
    return std::string_view((char*)this->ptr_ + position, length);
  }

  // compute hash of bytes range
  inline size_t hash(size_t position, size_t length) const noexcept {
    return Hasher::hash64(this->ptr_ + position, length);
  }

  template <typename T>
  inline size_t hash(size_t offset) const noexcept {
    constexpr size_t size = sizeof(T);
    return Hasher::hash64(ptr_ + offset, size);
  }

  inline int compare(size_t offset1, size_t offset2, size_t size) noexcept {
    return std::memcmp(ptr_ + offset1, ptr_ + offset2, size);
  }

  template <typename T>
  inline int compare(size_t offset1, size_t offset2) noexcept {
    constexpr size_t size = sizeof(T);
    return compare(offset1, offset2, size);
  }

  // capacity
  inline size_t capacity() const {
    return size_ * slices_;
  }

  // copy current slice data into a given buffer
  size_t copy(NByte*, size_t, size_t) const;

private:
  // ensure capacity of memory allocation
  void ensure(size_t);

  template <typename T>
  inline size_t
    writeSize(size_t position, const T& value, size_t size) {
    ensure(position + size);

    // a couple of options to copy the scalar type object into this memory address
    // 1. use reinterpret cast to assign the value directly
    // 2. use place new to construct the data there
    // not sure which one is better?
    *reinterpret_cast<T*>(this->ptr_ + position) = value;
    return size;
  }

private:
  size_t slices_;

  // recording total number of extension
  size_t numExtended_;
};

// TODO(cao): for int128_t, this line will crash in release build, fine with debug build
template <>
#ifndef __clang__
__attribute__((optimize("O1")))
#endif
inline size_t
  PagedSlice::writeSize(size_t position, const int128_t& value, size_t size) {
  ensure(position + size);
  *reinterpret_cast<int128_t*>(this->ptr_ + position) = value;
  return size;
}

// a basic range struct to hold offset and size in a size_t
struct Range {
  explicit Range() : Range(0, 0) {}
  explicit Range(uint32_t o, uint32_t s) : offset{ o }, size{ s } {}
  uint32_t offset;
  uint32_t size;

  inline bool include(size_t pos) const {
    return pos >= offset && pos < (offset + size);
  }

  // write range in a paged slice for given offset
  inline size_t write(PagedSlice& slice, size_t position) const {
    return write(slice, position, offset, size);
  }

  // write range in a paged slice for given offset
  static inline size_t write(PagedSlice& slice, size_t position, uint32_t offset, uint32_t size) {
    uint64_t value = offset;
    value = value << 32 | size;
    return slice.write(position, value);
  }

  inline void read(const PagedSlice& slice, size_t position) {
    uint64_t value = slice.read<uint64_t>(position);
    size = value;
    offset = (uint32_t)(value >> 32);
  }

  static inline Range make(const PagedSlice& slice, size_t position) {
    uint64_t value = slice.read<uint64_t>(position);
    return Range{ (uint32_t)(value >> 32), (uint32_t)value };
  }
};

// compression buffer will manage a fixed size buffer
// to receive input writes, when the buffer is full, it will compress it
// and output the compressed bytes into the designated slice, reset the buffer.
// it records the range of raw data for each compressed block
struct CompressionBlock {
  explicit CompressionBlock(Range r, std::unique_ptr<folly::IOBuf> d)
    : range{ std::move(r) }, data{ std::move(d) } {}
  Range range;
  std::unique_ptr<folly::IOBuf> data;
};

class CompressionSlice : public Slice {
  static constexpr auto EMPTY_STRING = "";

public:
  CompressionSlice(size_t size, folly::io::CodecType type = folly::io::CodecType::LZ4)
    : Slice{ size },
      write_{ 0, 0 },
      read_{ 0, 0 },
      type_{ type },
      codec_{ folly::io::getCodec(type) } {
    blocks_.reserve(64);
  }
  ~CompressionSlice() = default;

public:
  // append a bytes array of length bytes to position
  inline size_t write(size_t position, const char* data, size_t length) {
    return write(position, (const NByte*)data, length);
  }

  // write an external bytes buffer
  size_t write(size_t, const NByte*, size_t);

  // write a scalar data elements
  template <typename T>
  auto write(size_t position, const T& value) -> typename std::enable_if<std::is_scalar<T>::value, size_t>::type {
    constexpr size_t size = sizeof(T);
    // if current buffer can't fit the data, compress and flush the buffer
    if (write_.size + size > size_) {
      compress(position);
    }

    // write this value
    *reinterpret_cast<T*>(this->ptr_ + write_.size) = value;
    write_.size += size;
    return size;
  }

  // total memory allocation for current slice
  inline size_t size() const {
    return std::accumulate(blocks_.begin(), blocks_.end(), size_, [](size_t init, const CompressionBlock& b) {
      return init + b.data->length();
    });
  }

  // capacity
  inline size_t capacity() const {
    return size();
  }

  // read a scalar type
  template <typename T>
  typename std::enable_if<std::is_scalar<T>::value, T>::type read(size_t position) const {
    // if write buffer has the item
    if (write_.include(position)) {
      return *reinterpret_cast<T*>(this->ptr_ + position - write_.offset);
    }

    // check if position is in current range
    if (!read_.include(position)) {
      uncompress(position);
    }

    // buffer index = position - range.offset
    return *reinterpret_cast<T*>(buffer_->ptr() + position - read_.offset);
  }

  // read a string
  std::string_view read(size_t, size_t) const;

private:
  // ensure the buffer is big enough to hold single item
  void ensure(size_t);

  // compress current buffer and link it to the chain
  // recording the data range for this block [x-index_, x]
  void compress(size_t);

  // uncompress the compression block covers given position
  // and load it into current buffer (ptr_)
  void uncompress(size_t) const;

private:
  // write index in current buffer
  Range write_;

  // compressed blocks, to reduce block locating time,
  // we should keep number of blocks as small as possible, ideal size <32
  std::vector<CompressionBlock> blocks_;

  // TODO(cao) - we may introduce a reader to have these states rather than mix them here
  // indicating what range of raw data current buffer holds the uncompressed data
  Range read_;

  // a buffer used for hold raw data of any uncompressed block
  std::unique_ptr<OneSlice> buffer_;

  // the codec used to compress the buffer
  folly::io::CodecType type_;
  std::unique_ptr<folly::io::Codec> codec_;
};

} // namespace common
} // namespace nebula