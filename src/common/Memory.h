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

#include <folly/compression/Compression.h>
#include <glog/logging.h>
#include <iostream>
#include <numeric>
#include <vector>

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

// maintain a memory pool tracking memory chunks
// it gurantees memory are set to 0 for all allocated chunks through `memset`.
class Pool {
public:
  virtual ~Pool() = default;

  inline void* allocate(size_t size) {
    allocated_ += size;
    return std::memset(std::malloc(size), 0, size);
  }

  inline void free(void* p, size_t size) {
    freed_ += size;
    std::free(p);
  }

  void* extend(void* p, size_t size, size_t newSize) {
    N_ENSURE_GT(newSize, size, "new size should be larger than original size");

    // extend the memory if possible
    NByte* newP = (NByte*)std::realloc(p, newSize);
    if (UNLIKELY(!newP)) {
      free(p, size);
      throw std::bad_alloc();
    }

    auto delta = newSize - size;
    extended_ += delta;
    std::memset(newP + size, 0, delta);
    return newP;
  }

  std::string report() const {
    return fmt::format("Allocated:{0}, Extended:{1}, Freed:{2}", allocated_, extended_, freed_);
  }

  static Pool& getDefault();

private:
  // TODO(cao): a start point of pool impl, need a better pool management
  Pool() : allocated_{ 0 }, extended_{ 0 }, freed_{ 0 } {}

  size_t allocated_;
  size_t extended_;
  size_t freed_;
};

enum class SliceType {
  PAGED,
  SINGLE,
  COMPRESSION
};

/**
 * A slice represents a N sized memory chunk.
 * By default, a 64K page is provided. 
 */
class Slice {
public:
  virtual ~Slice() {
    if (ownbuffer_) {
      if (ptr_ != nullptr) {
        pool_.free(static_cast<void*>(ptr_), size_);
      }

      // when a slice is sealed - we may release it and ptr_ is nullptr
      // LOG(ERROR) << "A slice should hold a valid pointer";
    }
  }

  inline size_t size() const {
    return size_;
  }

  inline NByte* ptr() const {
    return ptr_;
  }

  inline void reset() const {
    std::memset(ptr_, 0, size_);
  }

protected:
  // A read-only slice!! wrapping an external buffer but not owning it
  Slice(const NByte* buffer, size_t size, bool own = false)
    : pool_{ Pool::getDefault() },
      size_{ size },
      ptr_{ const_cast<NByte*>(buffer) },
      ownbuffer_{ own } {}
  Slice(size_t size)
    : pool_{ Pool::getDefault() },
      size_{ size },
      ptr_{ static_cast<NByte*>(pool_.allocate(size)) },
      ownbuffer_{ true } {}
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
 * An extendable slice is whole chunk to writing size by keep allocation.
 */
class ExtendableSlice : public Slice {
public:
  ExtendableSlice(const NByte* buffer, size_t size) : Slice{ buffer, size }, numExtended_{ 0 } {}
  ExtendableSlice(size_t size) : Slice{ size }, numExtended_{ 0 } {}
  ~ExtendableSlice() = default;

  // append a bytes array of length bytes to position
  inline size_t write(size_t position, const char* data, size_t length) {
    return write(position, (NByte*)data, length);
  }

  // write byte array to given position and length
  size_t write(size_t, const NByte*, size_t);

  // write bits into the memory
  size_t writeBits(size_t, int, size_t);

  // append a data object at position using sizeof to determine its size
  template <typename T>
  inline auto write(size_t position, const T& value) -> typename std::enable_if<std::is_scalar<T>::value, size_t>::type {
    // write a scalar typed data in a given alignment space
    // auto size = std::max(sizeof(T), alignment);
    constexpr size_t size = sizeof(T);
    return writeSize(position, value, size);
  }

  template <typename T>
  inline size_t writeAlign(size_t position, const T& value, size_t alignment) {
    // write a scalar typed data in a given alignment space
    // auto size = std::max(sizeof(T), alignment);
    constexpr size_t size = sizeof(T);
    return writeSize(position, value, std::max(size, alignment));
  }

  // NOTE: (found a g++ bug)
  // It declares the method is not mark as const if we change the signature as
  // auto read(size_t position) -> typename std::enable_if<std::is_scalar<T>::value, T&>::type const {
  template <typename T>
  inline typename std::enable_if<std::is_scalar<T>::value, T>::type read(size_t position) const {
    return *reinterpret_cast<T*>(this->ptr_ + position);
  }

  inline std::string_view read(size_t position, size_t length) const {
    // build data using copy elision
    return std::string_view((char*)this->ptr_ + position, length);
  }

  // read bits from positin and bits width
  size_t readBits(size_t, int) const;

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

  // copy current slice data into a given buffer
  size_t copy(NByte*, size_t, size_t) const;

  // seal the slice - reduce memory if unused
  void seal(size_t);

private:
  // ensure capacity of memory allocation
  template <bool CHECK = false>
  inline void ensure(const size_t desired) {
    static constexpr size_t errors[] = { 8, 16 };
    if (UNLIKELY(desired >= size_)) {
      // start with 2 times
      auto fold = 2.0;
      if (numExtended_ > 8) {
        fold = 1.5;
      } else if (numExtended_ > 16) {
        fold = 1.2;
      }

      const auto numExtended = numExtended_;
      size_t size = size_ == 0 ? std::max<size_t>(1024, desired) : (fold * size_);
      while (size < desired) {
        ++numExtended_;
        size *= fold;

        if constexpr (UNLIKELY(CHECK)) {
          const auto count = numExtended_ - numExtended;
          if (count >= errors[0]) {
            LOG(WARNING) << "Slices increased too fast in single request";

            // over error bound - fail it
            if (UNLIKELY(count > errors[1])) {
              LOG(FATAL) << "Too fast allocation from " << size_ << " towards " << size;
            }
          }
        }
      }

      this->ptr_ = static_cast<NByte*>(this->pool_.extend(this->ptr_, size_, size));
      std::swap(size, size_);
    }
  }

  template <typename T>
  inline size_t writeSize(size_t position, const T& value, size_t size) {
    ensure(position + size);

    // a couple of options to copy the scalar type object into this memory address
    // 1. use reinterpret cast to assign the value directly
    // 2. use place new to construct the data there
    // not sure which one is better?
    *reinterpret_cast<T*>(this->ptr_ + position) = value;
    return size;
  }

private:
  // recording total number of extension
  size_t numExtended_;
};

// TODO(cao): for int128_t, this line will crash in release build, fine with debug build
template <>
#ifndef __clang__
__attribute__((optimize("O1")))
#endif
inline size_t
  ExtendableSlice::writeSize(size_t position, const int128_t& value, size_t size) {
  ensure(position + size);
  *reinterpret_cast<int128_t*>(this->ptr_ + position) = value;
  return size;
}

// a basic range struct to hold offset and size in a size_t
template <typename T>
struct Range {
  explicit Range() : Range(0, 0) {}
  explicit Range(uint32_t o, uint32_t s) : offset{ o }, size{ s } {}
  uint32_t offset;
  uint32_t size;

  inline bool include(size_t pos) const {
    return pos >= offset && pos < (offset + size);
  }

  // write range in a paged slice for given offset
  inline size_t write(T& slice, size_t position) const {
    return write(slice, position, offset, size);
  }

  // write range in a paged slice for given offset
  static inline size_t write(T& slice, size_t position, uint32_t offset, uint32_t size) {
    uint64_t value = offset;
    value = value << 32 | size;
    return slice.write(position, value);
  }

  inline void read(const T& slice, size_t position) {
    uint64_t value = slice.template read<uint64_t>(position);
    size = value;
    offset = (uint32_t)(value >> 32);
  }

  static inline Range make(const T& slice, size_t position) {
    uint64_t value = slice.template read<uint64_t>(position);
    return Range{ (uint32_t)(value >> 32), (uint32_t)value };
  }
};

class PagedSlice;
using PRange = Range<ExtendableSlice>;
using CRange = Range<PagedSlice>;

// compression buffer will manage a fixed size buffer
// to receive input writes, when the buffer is full, it will compress it
// and output the compressed bytes into the designated slice, reset the buffer.
// it records the range of raw data for each compressed block
struct CompressionBlock {
  explicit CompressionBlock(CRange r, bool c, std::unique_ptr<OneSlice> d)
    : range{ std::move(r) }, compressed{ c }, data{ std::move(d) } {}
  CRange range;
  bool compressed;
  std::unique_ptr<OneSlice> data;
};

// TODO(cao) - not thread-safe unless introducing thread-local state management
// Or introduce special read interface to allow caller manager reading block state
class PagedSlice : public Slice {
  static constexpr auto EMPTY_STRING = "";

public:
  PagedSlice(size_t size, folly::io::CodecType type = folly::io::CodecType::LZ4)
    : Slice{ size },
      write_{ 0, 0 },
      bid_{ 0 },
      bufferPtr_{ nullptr },
      type_{ type },
      codec_{ folly::io::getCodec(type) } {
  }
  ~PagedSlice() = default;

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
    if (UNLIKELY(write_.size + size > size_)) {
      compress(position);
    }

    // write this value
    *reinterpret_cast<T*>(this->ptr_ + write_.size) = value;
    write_.size += size;
    return size;
  }

  // total memory allocation for current slice
  inline size_t size() const {
    // 2*size_ to count for the read/write buffers
    return std::accumulate(blocks_.begin(), blocks_.end(), 2 * size_, [](size_t init, const CompressionBlock& b) {
      return init + b.data->size();
    });
  }

  // read a scalar type
  template <typename T>
  typename std::enable_if<std::is_scalar<T>::value, T>::type read(size_t position) const {
    if (UNLIKELY(this->ptr_ != nullptr)) {
      if (write_.include(position)) {
        return *reinterpret_cast<T*>(this->ptr_ + position - write_.offset);
      }
    }

    // check if position is in current range
    if (UNLIKELY(!bufferPtr_ || !blocks_.at(bid_).range.include(position))) {
      uncompress(position);
    }

    // buffer index = position - range.offset
    return *reinterpret_cast<T*>(bufferPtr_ + position - blocks_.at(bid_).range.offset);
  }

  // read a string
  std::string_view read(size_t, size_t) const;

  // seal the slice and no more writes expected
  void seal();

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
  CRange write_;

  // compressed blocks, to reduce block locating time,
  // we should keep number of blocks as small as possible, ideal size <32
  std::vector<CompressionBlock> blocks_;

  // TODO(cao) - not thread-safe as every thread can modify it
  // indicating a current block index in blocks_
  size_t bid_;

  // a buffer used for hold raw data of any uncompressed block
  std::unique_ptr<OneSlice> buffer_;
  // the buffer pointer may point to buffer_ which holds uncompressed data
  // or pointing to the original buffer itself if it's not compressed
  NByte* bufferPtr_;

  // the codec used to compress the buffer
  folly::io::CodecType type_;
  std::unique_ptr<folly::io::Codec> codec_;
};

} // namespace common
} // namespace nebula