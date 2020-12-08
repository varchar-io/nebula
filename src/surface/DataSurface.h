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
#include <optional>
#include <string_view>

#include "common/Cursor.h"
#include "common/Errors.h"
#include "common/Int128.h"

#include "eval/Aggregator.h"

/**
 * Define a Row data API to read data from
 * Row Data is basically a struct, it provides methods to retrieve all types of fields.
 * It's debatable to use what as field identifier to retrieve each field data, 
 * Especially when schema evolution comes into the scenario, mark a pending decision here.
 */
namespace nebula {
namespace surface {

// Supported compound types
class RowData;
class ListData;
class MapData;

// define a row cursor type, using pointer to allow poly
using IndexType = size_t;
using RowCursor = nebula::common::Cursor<RowData>;
using CompositeRowCursor = nebula::common::CompositeCursor<RowData>;
using RowCursorPtr = typename std::shared_ptr<RowCursor>;

class EmptyRowCursor : public RowCursor {
public:
  EmptyRowCursor() : RowCursor(0) {}
  virtual ~EmptyRowCursor() = default;

  // TODO(cao) - might be too expensive if there are many items/rows to iterate on
  virtual const RowData& next() override {
    throw NException("Empty cursor");
  }

  // a const interface return an unique ptr for secure randome access
  virtual std::unique_ptr<RowData> item(size_t) const override {
    throw NException("Empty cursor");
  }

  static RowCursorPtr instance() {
    static RowCursorPtr inst = std::make_shared<EmptyRowCursor>();
    return inst;
  }
};

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
  virtual int128_t readInt128(const std::string& field) const = 0;
  virtual std::string_view readString(const std::string& field) const = 0;

  // compound types
  virtual std::unique_ptr<ListData> readList(const std::string& field) const = 0;
  virtual std::unique_ptr<MapData> readMap(const std::string& field) const = 0;
  // NOT SUPPORT struct of struct type for now
  // virtual RowData readStruct(const std::string& field) const = 0;

  // this pointer will return whatever type of aggregator it has, otherwise return nullptr
  virtual std::shared_ptr<eval::Sketch> getAggregator(const std::string&) const {
    return nullptr;
  }

/////////////////////////////////////////////////////////////////////////////////////////////////
#define NOT_IMPL_FUNC(TYPE, FUNC)                                 \
  virtual TYPE FUNC(IndexType) const {                            \
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
  NOT_IMPL_FUNC(int128_t, readInt128)
  NOT_IMPL_FUNC(std::string_view, readString)
  NOT_IMPL_FUNC(std::unique_ptr<ListData>, readList)
  NOT_IMPL_FUNC(std::unique_ptr<MapData>, readMap)

  virtual std::shared_ptr<eval::Sketch> getAggregator(IndexType) const {
    return nullptr;
  }
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
  virtual int128_t readInt128(IndexType index) const = 0;
  virtual std::string_view readString(IndexType index) const = 0;

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

// for low level data access - avoid row data interface for possible optimization
class Accessor {
public:
  virtual ~Accessor() = default;
  virtual std::optional<bool> readBool(const std::string&) const = 0;
  virtual std::optional<int8_t> readByte(const std::string&) const = 0;
  virtual std::optional<int16_t> readShort(const std::string&) const = 0;
  virtual std::optional<int32_t> readInt(const std::string&) const = 0;
  virtual std::optional<int64_t> readLong(const std::string&) const = 0;
  virtual std::optional<float> readFloat(const std::string&) const = 0;
  virtual std::optional<double> readDouble(const std::string&) const = 0;
  virtual std::optional<int128_t> readInt128(const std::string&) const = 0;
  virtual std::optional<std::string_view> readString(const std::string&) const = 0;
};

} // namespace surface
} // namespace nebula
