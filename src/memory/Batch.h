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
#include "DataNode.h"
#include "DataSurface.h"
#include "Type.h"

/**
 * A batch defines a logical data block represents N rows for a given table/category.
 * For simplicity, a batch has fixed schema - 
 * (to support schema compatibility, a table/category can have different batches with different schemas)
 * 
 * 
 */
namespace nebula {
namespace memory {

using nebula::memory::DataNode;
using nebula::memory::DataTree;
using nebula::surface::ListData;
using nebula::surface::MapData;
using nebula::surface::RowData;
using nebula::type::Schema;

class Batch {
public:
  Batch(const Schema& schema) : schema_{ schema }, data_{ DataNode::buildDataTree(schema) }, rows_{ 0 } {
    // build data tree nodes
  }
  virtual ~Batch() = default;

  // add a row into current batch
  uint32_t add(const RowData& row);

  // random access to a row - may require internal seek
  RowData& row(uint32_t rowId);

  // get total rows in the batch
  uint32_t getRows() const {
    return rows_;
  }

private:
  Schema schema_;
  DataTree data_;
  uint32_t rows_;

  // friend class or nested class
  friend class RowAccessor;

  // (TODO) replace with batch cursor implementatation
  surface::MockRowData cursor_;
};

class RowAccessor : public RowData {
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
};

class ListAccessor : public ListData {
public:
  ListAccessor(uint32_t items) : ListData(items) {}
  bool isNull(uint32_t index) const override;
  bool readBool(uint32_t index) const override;
  std::int8_t readByte(uint32_t index) const override;
  int16_t readShort(uint32_t index) const override;
  int32_t readInt(uint32_t index) const override;
  int64_t readLong(uint32_t index) const override;
  float readFloat(uint32_t index) const override;
  double readDouble(uint32_t index) const override;
  std::string readString(uint32_t index) const override;
};

class MapAccessor : public MapData {
public:
  MapAccessor(uint32_t items) : MapData(items) {}
  std::unique_ptr<ListData> readKeys() const override;
  std::unique_ptr<ListData> readValues() const override;
};

} // namespace memory
} // namespace nebula