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
#include "DataNode.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * A batch defines a logical data block represents N rows for a given table/category.
 * For simplicity, a batch has fixed schema - 
 * (to support schema compatibility, a table/category can have different batches with different schemas)
 */
namespace nebula {
namespace memory {

using nebula::memory::DataNode;
using nebula::memory::DataTree;
using nebula::surface::ListData;
using nebula::surface::MapData;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeBase;
using DnMap = std::unordered_map<std::string, PDataNode>;
using nebula::surface::IndexType;

class RowAccessor;
class Batch {
public: // read row from and write row to
  Batch(const Schema&);
  virtual ~Batch() = default;

  // add a row into current batch
  size_t add(const RowData& row);

  // random access to a row - may require internal seek
  std::unique_ptr<RowAccessor> makeAccessor() const;

public: // basic metrics / meta of the batch
  // get total rows in the batch
  inline size_t getRows() const {
    return rows_;
  }

  // basic metrics in JSON
  std::string state() const;

private:
  Schema schema_;
  DataTree data_;
  size_t rows_;

  // A row accessor cursor to read data of given row
  friend class RowAccessor;

  // fast lookup from column name to column index
  DnMap fields_;
};

class RowAccessor : public RowData {
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
  std::string readString(const std::string& field) const override;

  // compound types
  std::unique_ptr<ListData> readList(const std::string& field) const override;
  std::unique_ptr<MapData> readMap(const std::string& field) const override;

public:
  RowAccessor& seek(size_t);

private:
  const Batch& batch_;
  const DnMap& dnMap_;
  size_t current_;
};

class ListAccessor : public ListData {
public:
  ListAccessor(IndexType offset, IndexType items, PDataNode node)
    : ListData(items), node_{ node }, offset_{ offset } {}
  bool isNull(IndexType index) const override;
  bool readBool(IndexType index) const override;
  std::int8_t readByte(IndexType index) const override;
  int16_t readShort(IndexType index) const override;
  int32_t readInt(IndexType index) const override;
  int64_t readLong(IndexType index) const override;
  float readFloat(IndexType index) const override;
  double readDouble(IndexType index) const override;
  std::string readString(IndexType index) const override;

private:
  PDataNode node_;
  IndexType offset_;
};

class MapAccessor : public MapData {
public:
  MapAccessor(IndexType items) : MapData(items) {}
  std::unique_ptr<ListData> readKeys() const override;
  std::unique_ptr<ListData> readValues() const override;
};

} // namespace memory
} // namespace nebula