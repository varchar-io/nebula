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

#include <glog/logging.h>
#include <iostream>
#include "Errors.h"
#include "Hash.h"
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

protected:
  // A read-only slice!! wrapping an external buffer but not owning it
  Slice(const NByte* buffer, size_t size) : pool_{ Pool::getDefault() }, size_{ size }, ptr_{ const_cast<NByte*>(buffer) }, ownbuffer_{ false } {}
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

/**
 * A paged slice is a chain of slices with given sized slice of chunks.
 * A paged slice is a slice, and it can have more slices as extensions when necessary.
 */
class PagedSlice : public Slice {
public:
  PagedSlice(const NByte* buffer, size_t size) : Slice{ buffer, size }, slices_{ 1 } {}
  PagedSlice(size_t page) : Slice{ page }, slices_{ 1 } {}
  ~PagedSlice() = default;

  // append a bytes array of length bytes to position
  inline size_t write(size_t position, const char* data, size_t length) {
    return write(position, (NByte*)data, length);
  }

  size_t write(size_t position, const NByte* data, size_t length);

  // append a data object at position using sizeof to determine its size
  template <typename T>
  auto write(size_t position, const T& value) -> typename std::enable_if<std::is_scalar<T>::value, size_t>::type {
    constexpr size_t size = sizeof(T);
    ensure(position + size);

    // a couple of options to copy the scalar type object into this memory address
    // 1. use reinterpret cast to assign the value directly
    // 2. use place new to construct the data there
    // not sure which one is better?
    *reinterpret_cast<T*>(this->ptr_ + position) = value;

    return size;
  }

  // NOTE: (found a g++ bug)
  // It declares the method is not mark as const if we change the signature as
  // auto read(size_t position) -> typename std::enable_if<std::is_scalar<T>::value, T&>::type const {
  template <typename T>
  typename std::enable_if<std::is_scalar<T>::value, T&>::type read(size_t position) const {
    constexpr size_t size = sizeof(T);
    N_ENSURE(position + size <= capacity(), "invalid position to read data");

    return *reinterpret_cast<T*>(this->ptr_ + position);
  }

  std::string_view read(size_t position, size_t length) const {
    N_ENSURE(position + length <= capacity(),
             fmt::format("invalid position ({0}, {1}) to read data", position, length));

    // build data using copy elision
    return std::string_view((char*)this->ptr_ + position, length);
  }

  // compute hash of bytes range
  size_t hash(size_t position, size_t length) const {
    return Hasher::hash64(this->ptr_ + position, length);
  }

  // capacity
  size_t capacity() const {
    return size_ * slices_;
  }

  // copy current slice data into a given buffer
  size_t copy(NByte*, size_t, size_t) const;

private:
  // ensure capacity of memory allocation
  void ensure(size_t);

private:
  size_t slices_;
};

} // namespace common
} // namespace nebula