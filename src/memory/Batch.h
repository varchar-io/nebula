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

#include <string_view>

#include "DataNode.h"

#include "meta/Table.h"
#include "surface/DataSurface.h"
#include "surface/eval/Histogram.h"
#include "surface/eval/ValueEval.h"
#include "type/Type.h"

/**
 * A batch defines a logical data block represents N rows for a given table/category.
 * For simplicity, a batch has fixed schema - 
 * (to support schema compatibility, a table/category can have different batches with different schemas)
 */
namespace nebula {
namespace memory {

using DnMap = nebula::common::unordered_map<std::string, PDataNode>;

class RowAccessor;
class Batch : public nebula::surface::eval::Block {
public: // read row from and write row to
  Batch(const nebula::meta::Table&, size_t capacity, size_t pid = 0);
  virtual ~Batch() = default;

  // add a row into current batch
  size_t add(const nebula::surface::RowData& row, nebula::meta::BessType bess = 0);

  // random access to a row - may require internal seek
  std::unique_ptr<RowAccessor> makeAccessor() const;

public: /* implement interface of Block.h */
  // get total rows in the batch
  inline size_t getRows() const override {
    return rows_;
  }

  nebula::type::TypeNode columnType(const std::string& col) const override {
    return schema_->find(col);
  }

  const nebula::surface::eval::Histogram& histogram(const std::string& col) const override {
    return fields_.at(col)->histogram();
  }

  std::vector<std::any> partitionValues(const std::string& col) const override {
    // if block is not partitioned
    if (pod_ == nullptr) {
      return {};
    }

#define DISPATCH_KIND(KIND)                                                                              \
  case nebula::type::Kind::KIND: {                                                                       \
    auto list = pod_->values<nebula::type::TypeTraits<nebula::type::Kind::KIND>::CppType>(col, spaces_); \
    return std::vector<std::any>(list.begin(), list.end());                                              \
  }

    // if column is not partitioned column
    auto ct = schema_->find(col);
    switch (ct->k()) {
      DISPATCH_KIND(BOOLEAN)
      DISPATCH_KIND(TINYINT)
      DISPATCH_KIND(SMALLINT)
      DISPATCH_KIND(INTEGER)
      DISPATCH_KIND(BIGINT)
    case nebula::type::Kind::VARCHAR: {
      auto list = pod_->values<std::string>(col, spaces_);
      return std::vector<std::any>(list.begin(), list.end());
    }
    default:
      return {};
    }

#undef DISPATCH_KIND
  }

  bool probably(const std::string& col, std::any v) const override {
#define DISPATCH_KIND(KIND)                                                 \
  case nebula::type::Kind::KIND: {                                          \
    using ET = nebula::type::TypeTraits<nebula::type::Kind::KIND>::CppType; \
    return probably<ET>(col, std::any_cast<ET>(v));                         \
  }

    // if column is not partitioned column
    auto ct = schema_->find(col);
    switch (ct->k()) {
      DISPATCH_KIND(BOOLEAN)
      DISPATCH_KIND(TINYINT)
      DISPATCH_KIND(SMALLINT)
      DISPATCH_KIND(INTEGER)
      DISPATCH_KIND(BIGINT)
    default:
      return true;
    }

#undef DISPATCH_KIND
  }

public:
  inline size_t getMemory() const {
    return data_->storageAllocation();
  }

  inline size_t getSize() const {
    return data_->storageSize();
  }

  inline size_t getRawSize() const {
    return data_->rawSize();
  }

  // basic metrics in JSON
  std::string state() const;

  // Place seal on current batch when building
  // This helps release some necessary memory used in batch building
  void seal();

  // a bloom filter tester
  template <typename T>
  inline bool probably(const std::string& col, const T& value) const {
    return fields_.at(col)->probably(value);
  }

  // get a const reference of the histogram object for given column
  template <typename T = nebula::surface::eval::Histogram>
  inline auto histogram(const std::string& col) const ->
    typename std::enable_if_t<std::is_base_of_v<nebula::surface::eval::Histogram, T>, T> {
    // delegate the call to the node
    return fields_.at(col)->histogram<T>();
  }

private:
  nebula::type::Schema schema_;
  nebula::memory::DataTree data_;

  // encoding bess for each partition columns if partitioned
  // pod will not be null if partitioned
  // spaces stores space/slot index for eacch partition dimension
  std::shared_ptr<nebula::meta::Pod> pod_;
  size_t pid_;
  std::vector<size_t> spaces_;
  size_t bessBits_;
  nebula::common::ExtendableSlice bess_;

  // recording number of rows
  size_t rows_;

  // A row accessor cursor to read data of given row
  friend class RowAccessor;

  // fast lookup from column name to column index
  DnMap fields_;

  bool sealed_;
};

using BatchPtr = std::shared_ptr<Batch>;
using EvaledBlock = std::pair<Batch*, nebula::surface::eval::BlockEval>;

class RowAccessor : public nebula::surface::RowData {
public:
  RowAccessor(const Batch& batch) : batch_{ batch }, dnMap_{ batch_.fields_ } {}
  virtual ~RowAccessor() = default;

public:
  bool isNull(const std::string& field) const override;
  bool readBool(const std::string& field) const override;
  int8_t readByte(const std::string& field) const override;
  int16_t readShort(const std::string& field) const override;
  int32_t readInt(const std::string& field) const override;
  int64_t readLong(const std::string& field) const override;
  float readFloat(const std::string& field) const override;
  double readDouble(const std::string& field) const override;
  int128_t readInt128(const std::string& field) const override;
  std::string_view readString(const std::string& field) const override;

  // compound types
  std::unique_ptr<nebula::surface::ListData> readList(const std::string& field) const override;
  std::unique_ptr<nebula::surface::MapData> readMap(const std::string& field) const override;

public:
  RowAccessor& seek(size_t);

private:
  const Batch& batch_;
  const DnMap& dnMap_;
  size_t current_;
  nebula::meta::BessType bessValue_;
};

class ListAccessor : public nebula::surface::ListData {
public:
  ListAccessor(IndexType offset, IndexType items, PDataNode node)
    : nebula::surface::ListData(items), node_{ node }, offset_{ offset } {}
  bool isNull(IndexType index) const override;
  bool readBool(IndexType index) const override;
  std::int8_t readByte(IndexType index) const override;
  int16_t readShort(IndexType index) const override;
  int32_t readInt(IndexType index) const override;
  int64_t readLong(IndexType index) const override;
  float readFloat(IndexType index) const override;
  double readDouble(IndexType index) const override;
  int128_t readInt128(IndexType index) const override;
  std::string_view readString(IndexType index) const override;

private:
  PDataNode node_;
  IndexType offset_;
};

class MapAccessor : public nebula::surface::MapData {
public:
  MapAccessor(IndexType items) : nebula::surface::MapData(items) {}
  std::unique_ptr<nebula::surface::ListData> readKeys() const override;
  std::unique_ptr<nebula::surface::ListData> readValues() const override;
};

} // namespace memory
} // namespace nebula