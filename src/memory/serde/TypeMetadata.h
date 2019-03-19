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
#include "Likely.h"
#include "Type.h"
#include "roaring.hh"

namespace nebula {
namespace memory {
namespace serde {

/**
 * A metadata serde to desribe metadata for a given type.
 * This is a super set that works for any type. 
 * 
 * Eventually this metadata can be serailized into a flat buffer to persistence.
 */
class TypeMetadata {
  using CompoundItems = std::vector<IndexType>;
  static constexpr size_t N_ITEMS = 1024;

public:
  TypeMetadata(nebula::type::Kind kind)
    : rawSize_{ 0 },
      offsetSize_{ nebula::type::TypeBase::isScalar(kind) ?
                     nullptr :
                     std::make_unique<CompoundItems>() } {
    if (offsetSize_ != nullptr) {
      // first item always equals 0
      offsetSize_->reserve(N_ITEMS);
      offsetSize_->push_back(0);
    }
  };
  virtual ~TypeMetadata() = default;

public:
  inline void setNull(size_t index) {
    nulls_.add(index);
  }

  inline bool isNull(size_t index) {
    return nulls_.contains(index);
  }

  void setOffsetSize(size_t index, IndexType items) {
    auto size = offsetSize_->size();
    auto last = offsetSize_->at(size - 1);

    // NULLS in the hole
    if (LIKELY(index >= size)) {
      while (size++ <= index) {
        offsetSize_->push_back(last);
      }
    }

    // an accumulated value
    offsetSize_->push_back(last + items);
  }

  inline std::tuple<IndexType, IndexType> offsetSize(size_t index) const {
    auto offset = offsetSize_->at(index);
    auto length = offsetSize_->at(index + 1) - offset;
    return std::make_tuple(offset, length);
  }

private:
  // raw size of this node
  size_t rawSize_;

  // store all null positions
  // call runOptimize() to compress the bitmap when finalizing.
  Roaring nulls_;

  // list or map store number of items for each object
  // it stores accumulated values, so for any index, the child index is
  // Index(i) = items_[i+1] - items_[i]
  // an example of items_ (the first item is always zero):
  // 0, 5, 6, 6, 8, 14, 20
  // we have 6 entry with each entry having items: 5, 1, 0, 2, 6, 6
  // last entry contains total items
  // string/binary type data node will use it for the same purpose to track offset/size of each item
  // call shrink_to_fit to compress the vector when finalizing
  // this can be futher compressed to storage efficiency.
  // uint32_t should be big enough for number of items in each object
  std::unique_ptr<std::vector<IndexType>> offsetSize_;
};

} // namespace serde
} // namespace memory
} // namespace nebula