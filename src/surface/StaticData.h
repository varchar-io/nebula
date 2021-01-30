/*
 * Copyright 2017-present varchar.io
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

#include "DataSurface.h"

/**
 * 
 * Mock static row for schema "ROW<id:int, event:string, items:list<string>, flag:bool>"
 * This is used by test only - to hold data for verification.
 */
namespace nebula {
namespace surface {

#define NOT_IMPL_FUNC(TYPE, NAME)       \
  TYPE NAME(IndexType) const override { \
    throw NException("x");              \
  }

class StaticList : public ListData {
public:
  StaticList(std::vector<std::string> data) : ListData(data.size()), data_{ std::move(data) } {
  }

  bool isNull(IndexType) const override {
    return false;
  }

  std::string_view readString(IndexType index) const override {
    return data_.at(index);
  }

  NOT_IMPL_FUNC(bool, readBool)
  NOT_IMPL_FUNC(int8_t, readByte)
  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(int32_t, readInt)
  NOT_IMPL_FUNC(int64_t, readLong)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(double, readDouble)
  NOT_IMPL_FUNC(int128_t, readInt128)

private:
  std::vector<std::string> data_;
};

#undef NOT_IMPL_FUNC

#define NOT_IMPL_FUNC(TYPE, NAME)                \
  TYPE NAME(const std::string&) const override { \
    throw NException("x");                       \
  }

class StaticRow : public RowData {
public:
  StaticRow(
    int64_t t,
    int i,
    std::string_view s,
    std::unique_ptr<ListData> list,
    bool f,
    char b,
    int128_t i128,
    double d)
    : time_{ t }, id_{ i }, event_{ s }, flag_{ f }, byte_{ b }, i128_{ i128 }, d_{ d } {
    if (list != nullptr) {
      items_.reserve(list->getItems());
      for (size_t k = 0; k < items_.capacity(); ++k) {
        items_.push_back(std::string(list->readString(k)));
      }
    }
  }

  // All intrefaces - string type has RVO, copy elision optimization
  bool isNull(const std::string& field) const override {
    return (field == "items" && items_.size() == 0) || (field == "value" && byte_ % 2 == 0);
  }

  bool isNull(IndexType index) const override {
    return (index == 4 && items_.size() == 0) || (index == 6 && byte_ % 2 == 0);
  }

  bool readBool(const std::string&) const override {
    return flag_;
  }

  bool readBool(IndexType) const override {
    return flag_;
  }

  int8_t readByte(const std::string&) const override {
    return byte_;
  }

  int8_t readByte(IndexType) const override {
    return byte_;
  }

  int32_t readInt(const std::string&) const override {
    return id_;
  }

  int32_t readInt(IndexType) const override {
    return id_;
  }

  std::string_view readString(const std::string&) const override {
    return event_;
  }

  std::string_view readString(IndexType) const override {
    return event_;
  }

  int64_t readLong(const std::string&) const override {
    return time_;
  }

  int64_t readLong(IndexType) const override {
    return time_;
  }

  double readDouble(const std::string&) const override {
    return d_;
  }

  double readDouble(IndexType) const override {
    return d_;
  }

  int128_t readInt128(const std::string&) const override {
    return i128_;
  }

  int128_t readInt128(IndexType) const override {
    return i128_;
  }

  std::unique_ptr<ListData> readList(const std::string&) const override {
    return items_.size() == 0 ? nullptr : std::make_unique<StaticList>(items_);
  }

  std::unique_ptr<ListData> readList(IndexType) const override {
    return items_.size() == 0 ? nullptr : std::make_unique<StaticList>(items_);
  }

  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(std::unique_ptr<MapData>, readMap)

private:
  int64_t time_;
  int id_;
  std::string event_;
  std::vector<std::string> items_;
  bool flag_;
  char byte_;
  int128_t i128_;
  double d_;
};

// data row for TestPartitionedTable
struct StaticPartitionedRow : public nebula::surface::RowData {
  static const std::string& d1() {
    static std::vector<std::string> D1V{ "a", "b", "c", "e", "f", "g" };
    return D1V.at(nebula::common::Evidence::ticks() % D1V.size());
  }

  static int8_t d2() {
    static std::vector<int8_t> D2V{ 1, 2, 3, 4 };
    return D2V.at(nebula::common::Evidence::ticks() % D2V.size());
  }

  static int32_t d3() {
    static std::vector<int32_t> D3V{ 11, 12, 13 };
    return D3V.at(nebula::common::Evidence::ticks() % D3V.size());
  }

  explicit StaticPartitionedRow(int64_t t, int8_t v, double w)
    : time_{ t },
      d1_{ d1() },
      d2_{ d2() },
      d3_{ d3() },
      value_{ v },
      weight_{ w } {}
  // All intrefaces - string type has RVO, copy elision optimization
  bool isNull(const std::string&) const override {
    return false;
  }

  int8_t readByte(const std::string& f) const override {
    return f == "d2" ? d2_ : value_;
  }

  int32_t readInt(const std::string&) const override {
    return d3_;
  }

  std::string_view readString(const std::string&) const override {
    return d1_;
  }

  int64_t readLong(const std::string&) const override {
    return time_;
  }

  double readDouble(const std::string&) const override {
    return weight_;
  }

  NOT_IMPL_FUNC(bool, readBool)
  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(int128_t, readInt128)
  NOT_IMPL_FUNC(std::unique_ptr<ListData>, readList)
  NOT_IMPL_FUNC(std::unique_ptr<MapData>, readMap)

  int64_t time_;
  std::string d1_;
  int8_t d2_;
  int32_t d3_;
  int8_t value_;
  double weight_;
};

#undef NOT_IMPL_FUNC

} // namespace surface
} // namespace nebula
