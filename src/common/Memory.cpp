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

#include "Memory.h"

namespace nebula {
namespace common {

Pool& Pool::getDefault() {
  static Pool pool;
  return pool;
}

// not-threadsafe
void PagedSlice::ensure(size_t size) {
  // increase 10 slices requests, logging warning, increase over 30 slices requests, logging error.
  static constexpr int errors[] = { 10, 20 };
  if (UNLIKELY(size >= capacity())) {
    auto slices = slices_;
    auto detects = 0;
    while (slices * size_ <= size) {
      ++slices;

      if (UNLIKELY(++detects >= errors[0])) {
        if (UNLIKELY(detects == errors[0])) {
          LOG(WARNING) << "Slices increased too fast in single request";
        }

        // over error bound - fail it
        if (UNLIKELY(detects > errors[1])) {
          LOG(FATAL) << "Slices increased too fast: it is a code bug or page size is too small";
        }
      }
    }

    N_ENSURE_GT(slices, slices_, "required slices should be more than existing capacity");
    this->ptr_ = static_cast<char*>(this->pool_.extend(this->ptr_, capacity(), slices * size_));
    std::swap(slices, slices_);
  }
}

// append a bytes array of length bytes to position
size_t PagedSlice::write(size_t position, const char* data, size_t length) {
  size_t cursor = position + length;
  ensure(cursor);

  // copy data into given place
  std::memcpy(this->ptr_ + position, data, length);
  return length;
}

} // namespace common
} // namespace nebula