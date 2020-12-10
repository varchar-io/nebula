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

#include <unordered_map>

#include "common/Hash.h"
#include "common/Memory.h"

namespace nebula {
namespace memory {
namespace encode {
/**
 * Dictionary encoding for text values.
 * It works like this:
 * 1. Reading tokens from the text value separated by <space>
 * 2. It applies only first 255 characters for using one byte store position info.
 * 3. It only works for token which length is longer than 2.
 * 4. An encoded string are stored like this 
 *      [number dict items(0~255)][p0(0-255)][c0(0-65536)][p1(0-255)]...[pN(0-255)]
 */
class DictEncoder {
  // assuming dictionary item can not exceeding max integer
  using IndexType = int32_t;

  // all items share the same hash value
  using HashItems = std::unordered_multimap<size_t, IndexType>;

  // 6K page size per each dictinoary
  static constexpr auto INDICE_PAGE = 2048;
  static constexpr auto DICT_PAGE = 4096;
  static constexpr auto IndexWidth = sizeof(IndexType);

public:
  DictEncoder()
    : hashItems_{ std::make_unique<HashItems>() },
      offsets_{ INDICE_PAGE },
      dict_{ DICT_PAGE },
      items_{ 0 },
      size_{ 0 } {
    offsets_.write(0, 0);
  }
  // set item and return its index in dictionary
  int32_t set(std::string_view item) {
    // check if this item is already in our dictionary
    auto hash = nebula::common::Hasher::hash64(item.data(), item.size());
    auto range = hashItems_->equal_range(hash);
    for (auto it = range.first; it != range.second; ++it) {
      // get offset and length of given index

      const auto index = it->second;
      auto str = get(index);

      // string view equals
      if (item == str) {
        return index;
      }
    }

    // not found, we're adding this new item
    // write offset of the new item
    size_ += dict_.write(size_, item.data(), item.size());
    // add the hash value to the list for next value to lookup
    hashItems_->emplace(hash, items_);
    offsets_.write((items_ + 1) * IndexWidth, size_);
    return items_++;
  }

  // get the item by its index
  inline std::string_view get(int32_t index) const {
    const auto pos = index * IndexWidth;
    const auto pos2 = pos + IndexWidth;
    auto offset = offsets_.read<IndexType>(pos);
    auto offset2 = offsets_.read<IndexType>(pos2);
    return dict_.read(offset, offset2 - offset);
  }

  void seal() {
    // release the assitant data structure
    hashItems_ = nullptr;

    offsets_.seal((items_ + 1) * IndexWidth);
    dict_.seal(size_);
  }

private:
  std::unique_ptr<HashItems> hashItems_;

  // every value has offset and length of the dict item
  nebula::common::ExtendableSlice offsets_;
  // store all dictionary items in order
  nebula::common::ExtendableSlice dict_;
  // current size of the dictinaary slice
  int32_t items_;
  int32_t size_;
};
} // namespace encode
} // namespace memory
} // namespace nebula