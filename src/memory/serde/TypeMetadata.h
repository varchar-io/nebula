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

#include <roaring.hh>
#include <unordered_map>

#include "TypeData.h"
#include "common/Likely.h"
#include "memory/encode/DictEncoder.h"
#include "surface/eval/Histogram.h"
#include "type/Type.h"

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
  static constexpr size_t N_ITEMS = 4096;
  static constexpr auto INDEX_WIDTH = sizeof(IndexType);

public:
  static constexpr IndexType INVALID_INDEX = std::numeric_limits<IndexType>::max();
  TypeMetadata(nebula::type::Kind kind, const nebula::meta::Column& column)
    : partition_{ column.partition.valid() },
      count_{ 0 },
      offsetSize_{
        nebula::type::TypeBase::isScalar(kind) ?
          nullptr :
          std::make_unique<nebula::common::ExtendableSlice>(N_ITEMS)
      },
      dict_{ column.withDict ? std::make_unique<nebula::memory::encode::DictEncoder>() : nullptr },
      default_{ column.defaultValue.size() > 0 },
      histo_{ nullptr } {

    if (offsetSize_ != nullptr) {
      // first item always equals 0
      offsetSize_->write<IndexType>(0, 0);
      ++count_;
    }

    // initialize histogram object
    histo_ = nullptr;
    bh_ = nullptr;
    ih_ = nullptr;
    rh_ = nullptr;
    switch (kind) {
    case nebula::type::Kind::BOOLEAN: {
      auto temp = std::make_unique<nebula::surface::eval::BoolHistogram>();
      bh_ = temp.get();
      histo_ = std::move(temp);
      break;
    }
    case nebula::type::Kind::TINYINT:
    case nebula::type::Kind::SMALLINT:
    case nebula::type::Kind::INTEGER:
    case nebula::type::Kind::BIGINT: {
      auto temp = std::make_unique<nebula::surface::eval::IntHistogram>();
      ih_ = temp.get();
      histo_ = std::move(temp);
      break;
    }
    case nebula::type::Kind::REAL:
    case nebula::type::Kind::DOUBLE: {
      auto temp = std::make_unique<nebula::surface::eval::RealHistogram>();
      rh_ = temp.get();
      histo_ = std::move(temp);
      break;
    }
    default:
      histo_ = std::make_unique<nebula::surface::eval::Histogram>();
      break;
    }
  }

  virtual ~TypeMetadata() = default;

public:
  inline void setNull(size_t index) {
    nulls_.add(index);
  }

  inline bool isNull(size_t index) {
    // column/node with default value will never be null
    if (default_) {
      return false;
    }

    return nulls_.contains(index);
  }

  inline bool isRealNull(size_t index) const {
    return default_ && nulls_.contains(index);
  }

  void setOffsetSize(size_t index, IndexType items) {
    auto last = offsetSize_->read<IndexType>((count_ - 1) * INDEX_WIDTH);

    // NULLS in the hole
    if (LIKELY(index >= count_)) {
      while (count_ <= index) {
        offsetSize_->write(count_ * INDEX_WIDTH, last);
        ++count_;
      }
    }

    // an accumulated value
    offsetSize_->write(count_++ * INDEX_WIDTH, last + items);
  }

  // no index link check, direct fetch offset and size
  inline std::pair<IndexType, IndexType> offsetSize(size_t index) const {
    auto iPos = index * INDEX_WIDTH;
    auto offset = offsetSize_->read<IndexType>(iPos);
    auto length = offsetSize_->read<IndexType>(iPos + INDEX_WIDTH) - offset;
    return { offset, length };
  }

  inline bool hasDict() const {
    return dict_ != nullptr;
  }

  inline int32_t dictItem(std::string_view item) {
    return dict_->set(item);
  }

  inline std::string_view dictItem(size_t index) {
    return dict_->get(index);
  }

  inline void seal() {
    // release hash items for lookup
    if (dict_) {
      dict_->seal();
    }

    // shrink bitmap
    nulls_.shrinkToFit();

    // seal the slice to release unused memory
    if (offsetSize_) {
      offsetSize_->seal(count_ * INDEX_WIDTH);
    }
  }

  inline bool hasDefault() const {
    return default_;
  }

  inline bool isPartition() const {
    return partition_;
  }

public:
  // build up histogram in metadata for supported types
  // including:
  //    bool type: true count, false count will be computed by "total - null count - true count"
  //    number type: min, max, count, sum (here valid count = "total - null count")
  //    string type: <TBD - total length?>
  template <typename T>
  size_t histogram(T);

  // get a const reference of the histogram object used internally
  template <typename T = nebula::surface::eval::Histogram>
  inline auto histogram() const ->
    typename std::enable_if_t<std::is_base_of_v<nebula::surface::eval::Histogram, T>, T> {
    return *static_cast<T*>(histo_.get());
  }

  const std::shared_ptr<nebula::surface::eval::Histogram> histogram() const {
    return histo_;
  }

private:
  // store all null positions
  // call runOptimize() to compress the bitmap when finalizing.
  roaring::Roaring nulls_;

  // save partition values within a space
  // it should be a few bits - but we start with non-compressed
  bool partition_;

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
  size_t count_;
  std::unique_ptr<nebula::common::ExtendableSlice> offsetSize_;

  // dictionary link one index to another index which has the value
  std::unique_ptr<nebula::memory::encode::DictEncoder> dict_;

  // indicate if this column has default value setting
  // if yes, it will never be NULL, default value will be returned instead of NULLs
  bool default_;

  // a histogram object storing concrete typed histogram
  // to avoid runtime casting, we use 3 different pointers internally pointing to the same object
  // they don't maintain referneces.
  std::shared_ptr<nebula::surface::eval::Histogram> histo_;
  nebula::surface::eval::BoolHistogram* bh_;
  nebula::surface::eval::IntHistogram* ih_;
  nebula::surface::eval::RealHistogram* rh_;
};

} // namespace serde
} // namespace memory
} // namespace nebula