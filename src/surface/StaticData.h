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

  std::string readString(IndexType index) const override {
    return data_.at(index);
  }

  NOT_IMPL_FUNC(bool, readBool)
  NOT_IMPL_FUNC(int8_t, readByte)
  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(int32_t, readInt)
  NOT_IMPL_FUNC(int64_t, readLong)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(double, readDouble)

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
  StaticRow(int64_t t, int i, std::string s, std::unique_ptr<ListData> list, bool f, char b)
    : time_{ t }, id_{ i }, event_{ std::move(s) }, flag_{ f }, byte_{ b } {
    if (list != nullptr) {
      items_.reserve(list->getItems());
      for (size_t k = 0; k < items_.capacity(); ++k) {
        items_.push_back(list->readString(k));
      }
    }
  }

  // All intrefaces - string type has RVO, copy elision optimization
  bool isNull(const std::string& field) const override {
    return field == "items" && items_.size() == 0;
  }

  bool isNull(IndexType index) const override {
    return index == 3 && items_.size() == 0;
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

  std::string readString(const std::string&) const override {
    return event_;
  }

  std::string readString(IndexType) const override {
    return event_;
  }

  int64_t readLong(const std::string&) const override {
    return time_;
  }

  int64_t readLong(IndexType) const override {
    return time_;
  }

  std::unique_ptr<ListData> readList(const std::string&) const override {
    return items_.size() == 0 ? nullptr : std::make_unique<StaticList>(items_);
  }

  std::unique_ptr<ListData> readList(IndexType) const override {
    return items_.size() == 0 ? nullptr : std::make_unique<StaticList>(items_);
  }

  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(double, readDouble)
  NOT_IMPL_FUNC(std::unique_ptr<MapData>, readMap)

#undef NOT_IMPL_FUNC

private:
  int64_t time_;
  int id_;
  std::string event_;
  std::vector<std::string> items_;
  bool flag_;
  char byte_;
};

} // namespace surface
} // namespace nebula
