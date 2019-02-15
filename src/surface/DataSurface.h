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
#include "Errors.h"

/**
 * Define a Row data API to read data from
 * Row Data is basically a struct, it provides methods to retrieve all types of fields.
 * It's debatable to use what as field identifier to retrieve each field data, 
 * Especially when schema evolution comes into the scenario, mark a pending decision here.
 */
namespace nebula {
namespace surface {

using namespace nebula::common;

// Supported compound types
class RowData;
class ListData;
class MapData;

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
};

class ListData {
public:
  ListData(uint32_t items) : items_{ items } {
    N_ENSURE(items > 0, "empty list treated as NULL");
  }
  virtual ~ListData() = default;

  int getItems() const { return items_; }

  // all interfaces - not support list of compounds types for now
  virtual bool isNull(uint32_t index) const = 0;
  virtual bool readBool(uint32_t index) const = 0;
  virtual int8_t readByte(uint32_t index) const = 0;
  virtual int16_t readShort(uint32_t index) const = 0;
  virtual int32_t readInt(uint32_t index) const = 0;
  virtual int64_t readLong(uint32_t index) const = 0;
  virtual float readFloat(uint32_t index) const = 0;
  virtual double readDouble(uint32_t index) const = 0;
  virtual std::string readString(uint32_t index) const = 0;

private:
  uint32_t items_;
};

class MapData {
public:
  MapData(uint32_t items) : items_{ items } {
    N_ENSURE(items > 0, "empty map treated as NULL");
  }
  virtual ~MapData() = default;

  int getItems() const { return items_; }

  // all interfaces - keys list and values list have the same number of items
  virtual std::unique_ptr<ListData> readKeys() const = 0;
  virtual std::unique_ptr<ListData> readValues() const = 0;

private:
  uint32_t items_;
};

////////////////////////////////////////////////// MOCK DATA //////////////////////////////////////////////////
// Supported compound types
class MockRowData : public RowData {
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

class MockListData : public ListData {
public:
  MockListData(uint32_t items) : ListData(items) {}
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

class MockMapData : public MapData {
public:
  MockMapData(uint32_t items) : MapData(items) {}
  std::unique_ptr<ListData> readKeys() const override;
  std::unique_ptr<ListData> readValues() const override;
};
} // namespace surface
} // namespace nebula
