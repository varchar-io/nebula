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

#include <cstdint>
#include "common/Cursor.h"
#include "common/Errors.h"
#include "common/Evidence.h"

/**
 * Define a Row data API to read data from
 * Row Data is basically a struct, it provides methods to retrieve all types of fields.
 * It's debatable to use what as field identifier to retrieve each field data, 
 * Especially when schema evolution comes into the scenario, mark a pending decision here.
 */
namespace nebula {
namespace surface {

using namespace nebula::common;
using IndexType = size_t;

// Supported compound types
class RowData;
class ListData;
class MapData;

// define a row cursor type, using pointer to allow poly
using RowCursor = typename std::shared_ptr<nebula::common::Cursor<RowData>>;

// (TODO) CRTP - avoid virtual methods?
class RowData {
public:
  RowData() {
    N_ENSURE(sizeof(float) == 4, "The system expects float to be 4 bytes");
    N_ENSURE(sizeof(double) == 8, "The system expects double to be 8 bytes");
  }
  virtual ~RowData() = default;

  // All intrefaces - string type has RVO, copy elision optimization
  virtual bool isNull(const std::string& field) const = 0;
  virtual bool readBool(const std::string& field) const = 0;
  virtual int8_t readByte(const std::string& field) const = 0;
  virtual int16_t readShort(const std::string& field) const = 0;
  virtual int32_t readInt(const std::string& field) const = 0;
  virtual int64_t readLong(const std::string& field) const = 0;
  virtual float readFloat(const std::string& field) const = 0;
  virtual double readDouble(const std::string& field) const = 0;
  virtual std::string readString(const std::string& field) const = 0;

  // compound types
  virtual std::unique_ptr<ListData> readList(const std::string& field) const = 0;
  virtual std::unique_ptr<MapData> readMap(const std::string& field) const = 0;
// NOT SUPPORT struct of struct type for now
// virtual RowData readStruct(const std::string& field) const = 0;

/////////////////////////////////////////////////////////////////////////////////////////////////
#define NOT_IMPL_FUNC(TYPE, FUNC)                                 \
  virtual TYPE FUNC(IndexType index) const {                      \
    throw NException(#FUNC " (IndexType index) not implemented"); \
  }

  NOT_IMPL_FUNC(bool, isNull)
  NOT_IMPL_FUNC(bool, readBool)
  NOT_IMPL_FUNC(int8_t, readByte)
  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(int32_t, readInt)
  NOT_IMPL_FUNC(int64_t, readLong)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(double, readDouble)
  NOT_IMPL_FUNC(std::string, readString)
  NOT_IMPL_FUNC(std::unique_ptr<ListData>, readList)
  NOT_IMPL_FUNC(std::unique_ptr<MapData>, readMap)

#undef NOT_IMPL_FUNC
};

class ListData {
public:
  ListData(IndexType items) : items_{ items } {
    N_ENSURE(items > 0, "empty list treated as NULL");
  }
  virtual ~ListData() = default;

  int getItems() const { return items_; }

  // all interfaces - not support list of compounds types for now
  virtual bool isNull(IndexType index) const = 0;
  virtual bool readBool(IndexType index) const = 0;
  virtual int8_t readByte(IndexType index) const = 0;
  virtual int16_t readShort(IndexType index) const = 0;
  virtual int32_t readInt(IndexType index) const = 0;
  virtual int64_t readLong(IndexType index) const = 0;
  virtual float readFloat(IndexType index) const = 0;
  virtual double readDouble(IndexType index) const = 0;
  virtual std::string readString(IndexType index) const = 0;

private:
  IndexType items_;
};

class MapData {
public:
  MapData(IndexType items) : items_{ items } {
    N_ENSURE(items > 0, "empty map treated as NULL");
  }
  virtual ~MapData() = default;

  int getItems() const { return items_; }

  // all interfaces - keys list and values list have the same number of items
  virtual std::unique_ptr<ListData> readKeys() const = 0;
  virtual std::unique_ptr<ListData> readValues() const = 0;

private:
  IndexType items_;
};

////////////////////////////////////////////////// MOCK DATA //////////////////////////////////////////////////
class MockData {
public:
  MockData(size_t seed = 0) : rand_{ nebula::common::Evidence::rand(seed == 0 ? Evidence::ticks() : seed) } {}
  virtual ~MockData() = default;

protected:
  std::function<double()> rand_;
};

// Supported compound types
class MockRowData : public RowData, protected MockData {
public:
  MockRowData(size_t seed = 0) : seed_{ seed }, MockData(seed) {}

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

  bool isNull(IndexType index) const override;
  bool readBool(IndexType index) const override;
  int8_t readByte(IndexType index) const override;
  int16_t readShort(IndexType index) const override;
  int32_t readInt(IndexType index) const override;
  int64_t readLong(IndexType index) const override;
  float readFloat(IndexType index) const override;
  double readDouble(IndexType index) const override;
  std::string readString(IndexType index) const override;

  // compound types
  std::unique_ptr<ListData> readList(IndexType index) const override;
  std::unique_ptr<MapData> readMap(IndexType index) const override;

private:
  size_t seed_;
};

class MockRowCursor : public nebula::common::Cursor<RowData> {
public:
  MockRowCursor() : Cursor<RowData>(8) {
  }

  virtual const RowData& next() {
    return rowData_;
  }

private:
  MockRowData rowData_;
};

class MockListData : public ListData, protected MockData {
public:
  MockListData(IndexType items, size_t seed = 0) : ListData(items), MockData(seed) {}
  bool isNull(IndexType index) const override;
  bool readBool(IndexType index) const override;
  std::int8_t readByte(IndexType index) const override;
  int16_t readShort(IndexType index) const override;
  int32_t readInt(IndexType index) const override;
  int64_t readLong(IndexType index) const override;
  float readFloat(IndexType index) const override;
  double readDouble(IndexType index) const override;
  std::string readString(IndexType index) const override;
};

class MockMapData : public MapData, protected MockData {
public:
  MockMapData(IndexType items, size_t seed = 0) : MapData(items), MockData(seed) {}
  std::unique_ptr<ListData> readKeys() const override;
  std::unique_ptr<ListData> readValues() const override;
};
} // namespace surface
} // namespace nebula
