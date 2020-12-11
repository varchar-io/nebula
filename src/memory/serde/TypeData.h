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

#include <cmath>
#include <glog/logging.h>

#include "common/BloomFilter.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "meta/Table.h"
#include "type/Type.h"

namespace nebula {
namespace memory {
namespace serde {

using IndexType = size_t;

// type metadata implementation for each type kind
template <nebula::type::Kind>
class TypeDataImpl;

/**
 * A data serde to desribe real data for a given type.
 * The base type acts like a proxy to delegate corresponding typed data.
 */
class TypeData {
public:
  TypeData() : size_{ 0 } {};
  virtual ~TypeData() = default;

public:
  inline size_t size() const {
    return size_;
  }

  virtual size_t capacity() const = 0;

  virtual void seal() = 0;

protected:
  // data size in slice_
  size_t size_;
};

// we have empty data because compound type node doesn't really hold data
// so instead we don't define data for any compound type
using EmptyData = TypeDataImpl<nebula::type::Kind::INVALID>;
using BoolData = TypeDataImpl<nebula::type::Kind::BOOLEAN>;
using ByteData = TypeDataImpl<nebula::type::Kind::TINYINT>;
using ShortData = TypeDataImpl<nebula::type::Kind::SMALLINT>;
using IntData = TypeDataImpl<nebula::type::Kind::INTEGER>;
using LongData = TypeDataImpl<nebula::type::Kind::BIGINT>;
using FloatData = TypeDataImpl<nebula::type::Kind::REAL>;
using DoubleData = TypeDataImpl<nebula::type::Kind::DOUBLE>;
using Int128Data = TypeDataImpl<nebula::type::Kind::INT128>;
using StringData = TypeDataImpl<nebula::type::Kind::VARCHAR>;

template <nebula::type::Kind KIND>
class TypeDataImpl : public TypeData {
  using NType = typename nebula::type::TypeTraits<KIND>::CppType;
  static constexpr auto Scalar = nebula::type::TypeBase::isScalar(KIND);
  static constexpr auto Unit = Scalar ? nebula::type::TypeTraits<KIND>::width : 16;

public:
  TypeDataImpl(const nebula::meta::Column&, size_t);
  virtual ~TypeDataImpl() = default;

public:
  void add(IndexType, NType value) {
    size_ += slice_.write(size_, value);
    if (UNLIKELY(bf_ != nullptr)) {
      if (!bf_->add(value)) {
        bf_ = nullptr;
        LOG(INFO) << "Failed to add value to bloom filter: " << size()
                  << ",  type=" << nebula::type::TypeTraits<KIND>::name;
      }
    }
  }

  void addVoid(IndexType) {
    size_ += slice_.write(size_, (NType)0);
  }

  NType read(IndexType index) const {
    return slice_.template read<NType>(index * Unit);
  }

  inline std::string_view read(IndexType offset, IndexType size) {
    return slice_.read(offset, size);
  }

  inline size_t capacity() const override {
    return slice_.size();
  }

  inline bool hasBloomFilter() const {
    return bf_ != nullptr;
  }

  bool probably(NType) const;

  NType defaultValue() const {
    return default_;
  }

  inline virtual void seal() override {
    slice_.seal(size_);
  }

private:
  // memory chunk managed by paged slice
  nebula::common::ExtendableSlice slice_;
  std::unique_ptr<nebula::common::BloomFilter<NType>> bf_;

  // default value of this data node
  NType default_;
};

/**
 * A proxy is used to dispatch different methods.
 * Bad part -> these functions can't be inline due to full specification of template.
 */

class TypeDataProxy {
  using PTypeData = std::unique_ptr<TypeData>;

public:
  TypeDataProxy(PTypeData);
  virtual ~TypeDataProxy() = default;

public:
  template <typename T>
  void add(IndexType, T);

  inline void addVoid(IndexType index) {
    if (LIKELY(void_ != nullptr)) {
      void_(index);
    }
  }

public:
  template <typename T>
  T read(IndexType) const;

  inline std::string_view read(IndexType offset, IndexType size) const {
    return std_->read(offset, size);
  }

  template <typename T>
  bool probably(T) const;

  template <typename T>
  T defaultValue() const;

public:
  inline size_t size() const {
    return data_->size();
  }

  inline size_t capacity() const {
    return data_->capacity();
  }

  inline bool hasBloomFilter() const {
    return hasBf_;
  }

  inline void seal() {
    data_->seal();
  }

private:
  // data_ is owned object while other plain pointers are internal refs
  PTypeData data_;
  BoolData* bd_;
  ByteData* btd_;
  ShortData* sd_;
  IntData* id_;
  LongData* ld_;
  FloatData* fd_;
  DoubleData* dd_;
  Int128Data* i128d_;
  StringData* std_;
  std::function<void(IndexType)> void_;
  bool hasBf_;
};

} // namespace serde
} // namespace memory
} // namespace nebula