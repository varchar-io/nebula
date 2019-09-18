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

#include <gflags/gflags.h>

DEFINE_bool(ALLOC_CHECK, false, "check allocation and fail it grows too much");

namespace nebula {
namespace common {

Pool& Pool::getDefault() {
  static Pool pool;
  return pool;
}

// not-threadsafe
void PagedSlice::ensure(size_t size) {
  // increase 10 slices requests, logging warning, increase over 30 slices requests, logging error.
  static constexpr size_t errors[] = { 50, 100 };
  if (UNLIKELY(size >= capacity())) {
    // TODO(cao): a bit prediction here, is this effective?
    // OR we just need real paged slices without continous memory chunk
    if (numExtended_ > 4) {
      // give 2 times of what asked
      size *= 2;
    } else if (numExtended_ > 8) {
      size *= 4;
    } else if (numExtended_ > 16) {
      size *= 8;
    }

    auto slices = slices_;
    while (slices * size_ <= size) {
      ++slices;

      if (FLAGS_ALLOC_CHECK) {
        auto detects = slices - slices_;
        if (detects >= errors[0]) {
          LOG(WARNING) << "Slices increased too fast in single request";

          // over error bound - fail it
          if (UNLIKELY(detects > errors[1])) {
            LOG(FATAL) << fmt::format(
              "Slices grows too fast: page size ({0}) too small or allocation leak towards {1}", size_, size);
          }
        }
      }
    }

    N_ENSURE_GT(slices, slices_, "required slices should be more than existing capacity");
    ++numExtended_;
    this->ptr_ = static_cast<NByte*>(this->pool_.extend(this->ptr_, capacity(), slices * size_));
    std::swap(slices, slices_);
  }
}

// append a bytes array of length bytes to position
size_t PagedSlice::write(size_t position, const NByte* data, size_t length) {
  if (UNLIKELY(length == 0)) {
    return 0;
  }

  size_t cursor = position + length;
  ensure(cursor);

  // copy data into given place
  std::memcpy(this->ptr_ + position, data, length);
  return length;
}

size_t PagedSlice::copy(NByte* buffer, size_t offset, size_t length) const {
  N_ENSURE(length <= capacity(), "requested data is too much.");

  // copy over
  std::memcpy(buffer + offset, this->ptr_, length);

  return length;
}

} // namespace common
} // namespace nebula