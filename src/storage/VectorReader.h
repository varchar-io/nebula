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

#include <folly/Conv.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "common/Cursor.h"
#include "common/Format.h"
#include "common/Hash.h"
#include "memory/FlatRow.h"
#include "surface/DataSurface.h"
#include "type/Serde.h"
#include "type/Type.h"

/**
 * A Column Vector formatted data reader. 
 * On top of this, we can have a file reader interface to load data from given file. 
 * 
 * Most of the case, the data is built / setup in memory already.
 * The data looks like below
 * [
 *  [c1, ...],
 *  [c2, ...],
 *  ...
 *  [cn, ...]
 * ]
 */
namespace nebula {
namespace storage {

// alias json array via rapidjson impl
using JsonArray = rapidjson::GenericArray<false, rapidjson::Value>;

// represents a json column
class VectorRow : public nebula::surface::RowData {
public:
  VectorRow(size_t& index)
    : index_{ index } {
  }

  // push a column vector data to the row wrapper
  void push(nebula::type::TypeNode node, JsonArray array) {
    // emplace in the map
    data_.emplace(node->name(), std::move(array));
  }

  bool isNull(const std::string& field) const override {
    return data_.find(field) == data_.end();
  }

#define CONV_TYPE_INDEX(TYPE, FUNC)                    \
  TYPE FUNC(const std::string& field) const override { \
    return this->template convert<TYPE>(field);        \
  }

  CONV_TYPE_INDEX(bool, readBool)
  CONV_TYPE_INDEX(int8_t, readByte)
  CONV_TYPE_INDEX(int16_t, readShort)
  CONV_TYPE_INDEX(int32_t, readInt)
  CONV_TYPE_INDEX(int64_t, readLong)
  CONV_TYPE_INDEX(float, readFloat)
  CONV_TYPE_INDEX(double, readDouble)
  CONV_TYPE_INDEX(int128_t, readInt128)

  std::string_view readString(const std::string& field) const override {
    static constexpr auto EMPTY = "";
    const auto& vector = data_.at(field);
    const auto realIndex = index();
    if (N_UNLIKELY(realIndex >= vector.Size())) {
      return EMPTY;
    }

    auto& value = vector[realIndex];
    if (value.IsNumber()) {
      // save this for string_view reference
      auto& snapshot = const_cast<VectorRow*>(this)->strs_;

      // TODO(cao): probably need more fine-grained typed data reading (rather than just double/int64)
      snapshot[field] = value.IsDouble() ?
                          std::to_string(value.GetDouble()) :
                          std::to_string(value.GetInt64());
      return snapshot.at(field);
    }

    return value.GetString();
  }

#undef CONV_TYPE_INDEX

  // compound types
  std::unique_ptr<nebula::surface::ListData> readList(const std::string&) const override {
    throw NException("Array not supported yet.");
  }

  std::unique_ptr<nebula::surface::MapData> readMap(const std::string&) const override {
    throw NException("Map not supported yet.");
  }

private:
  // NON-STRING target conversion
  template <typename T>
  T convert(const std::string& field) const {
    const auto& vector = data_.at(field);
    const auto realIndex = index();
    if (N_UNLIKELY(realIndex >= vector.Size())) {
      return T();
    }

    auto& value = vector[realIndex];
    if (value.IsNumber()) {
      return (T)value.GetDouble();
    }

    // try to convert from its string value
    // use default value if conversion failed
    try {
      return folly::to<T>(value.GetString());
    } catch (std::exception& ex) {
      return T();
    }
  }

  inline size_t index() const {
    // value materialization happens after next()
    // call which starts at 1 instead of 0
    return index_ - 1;
  }

private:
  size_t& index_;
  nebula::common::unordered_map<std::string, std::string> strs_;
  nebula::common::unordered_map<std::string, JsonArray> data_;
};

/**
 * A JSON vector reader with vector of column vectors.
 * schema - desired data schema, if not found in data, values will be null
 * values - vector of column vectors
 * size - number of data rows excluding header if present
 * headless - no head presents at the first row
 */
class JsonVectorReader : public nebula::surface::RowCursor {
public:
  JsonVectorReader(nebula::type::Schema schema,
                   const rapidjson::GenericArray<false, rapidjson::Value>& values,
                   size_t size,
                   bool headless = false)
    : nebula::surface::RowCursor(size),
      row_{ index_ } {
    // for each column vector
    using ST = rapidjson::SizeType;
    auto numColumns = std::min(values.Size(), (ST)schema->size());

    // sheet rows (size) doesn't really mean the data rows (many are empty not in data)
    // we need to find out the max rows of each column vector
    size_t maxRows = 0;
    LOG(INFO) << "Load data with schema=" << nebula::type::TypeSerializer::to(schema);
    for (ST i = 0; i < numColumns; ++i) {
      auto columnVector = values[i].GetArray();
      auto numRows = columnVector.Size();
      if (numRows > 0) {
        if (numRows > maxRows) {
          maxRows = numRows;
        }

        // if header is inline, use it to search column definition
        // otherwise, we will use the column by index
        auto node = schema->childType(i);
        if (!headless) {
          auto name = nebula::common::normalize(getTitle(columnVector[0]));
          node = schema->find(name);
          if (!node) {
            LOG(WARNING) << "Name not found in schema:" << name << "|";
          }
        }

        if (node) {
          row_.push(node, std::move(columnVector));
        }
      }
    }

    // if headless, index_ will be
    if (!headless) {
      index_++;
    }

    // use smaller value to load data
    if (maxRows < size) {
      size_ = maxRows;
    }

    // log a json vector reader creation
    LOG(INFO) << "Initialize a json vector reader for rows="
              << size
              << " without header="
              << headless;
  }
  virtual ~JsonVectorReader() = default;

public:
  // next row data of JsonsRow
  virtual const nebula::surface::RowData& next() override {
    index_++;
    return row_;
  }

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t) const override {
    throw NException("stream-based JSON Reader does not support random access.");
  }

private:
  inline std::string getTitle(const rapidjson::Value& vt) {
    if (vt.IsString()) {
      return vt.GetString();
    } else if (vt.IsInt()) {
      return std::to_string(vt.GetInt());
    } else if (vt.IsInt64()) {
      return std::to_string(vt.GetInt64());
    } else if (vt.IsDouble()) {
      return std::to_string(vt.GetDouble());
    } else {
      return "unknown";
    }
  }

private:
  VectorRow row_;
};

} // namespace storage
} // namespace nebula