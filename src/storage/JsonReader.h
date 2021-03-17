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

#include <fstream>
#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/pointer.h>
#include <string>

#include "RowParser.h"
#include "common/Chars.h"
#include "common/Conv.h"
#include "common/Errors.h"
#include "memory/FlatRow.h"
#include "meta/Table.h"
#include "surface/DataSurface.h"

/**
 * A JSON file reader, the expected file is list of json objects separated by new line.
 * It reads line by line as string, and let JSON parse each line as object
 */
namespace nebula {
namespace storage {

inline rapidjson::Value* locate(rapidjson::Document& doc, const std::string& path) {
  return rapidjson::Pointer(path.c_str()).Get(doc);
}

// define column properties
using fop = std::function<void(nebula::memory::FlatRow&, const std::string&, const rapidjson::Value*)>;
struct ColumnProps {
  // maybe empty - but storing parsed path if the name maps to a path
  std::string path;

  // action to convert data from json node to given storage (eg. FlatRow)
  fop action;
};

// represent a reusable row object with single line content
// we can always parse line for a row object
class JsonRow final : public RowParser {

public:
  // to support flat data from nested structure in JSON, introduce "pathInName" variable
  // if this is true, use name as path to match value otherwise use name itself
  // currently we default to support path in name.
  JsonRow(nebula::type::Schema schema, bool defaultNull = false)
    : defaultNull_{ defaultNull }, hasTime_{ false } {

// define how each column read and write to row object
// if the provided value is string, we use safe_to to convert it to desired type without exception
// other excpetions, we let it throw
#define CASE_POP(K, F)                                                                                            \
  case nebula::type::Kind::K: {                                                                                   \
    using T = nebula::type::TypeTraits<nebula::type::Kind::K>;                                                    \
    props_.emplace(name,                                                                                          \
                   ColumnProps{ nebula::common::Chars::path(name.data(), name.size()),                            \
                                [](nebula::memory::FlatRow& r, const std::string& n, const rapidjson::Value* v) { \
                                  if (v == nullptr || v->IsNull()) {                                              \
                                    r.write(n, nebula::type::TypeDetect<T::CppType>::value);                      \
                                  } else if (v->IsString()) {                                                     \
                                    r.write(n, nebula::common::safe_to<T::CppType>(v->GetString()));              \
                                  } else {                                                                        \
                                    r.write(n, (T::CppType)v->F());                                               \
                                  }                                                                               \
                                } });                                                                             \
    break;                                                                                                        \
  }

    for (size_t i = 0; i < schema->size(); ++i) {
      auto type = schema->childType(i);
      const auto& name = type->name();
      if (name == nebula::meta::Table::TIME_COLUMN) {
        hasTime_ = true;
      }

      switch (type->k()) {
        CASE_POP(BOOLEAN, GetBool)
        CASE_POP(TINYINT, GetInt)
        CASE_POP(SMALLINT, GetInt)
        CASE_POP(INTEGER, GetInt)
        CASE_POP(BIGINT, GetInt64)
        CASE_POP(REAL, GetFloat)
        CASE_POP(DOUBLE, GetDouble)
      case nebula::type::Kind::VARCHAR:
        props_.emplace(name,
                       ColumnProps{ nebula::common::Chars::path(name.data(), name.size()),
                                    [](nebula::memory::FlatRow& r, const std::string& n, const rapidjson::Value* v) {
                                      // is null or is not expected string type (malformed data) - Nebula enforce types.
                                      // we have chance to compatible with other types and convert them into string, such as numbers.
                                      if (v == nullptr || v->IsNull() || !v->IsString()) {
                                        r.write(n, "");
                                      } else {
                                        r.write(n, v->GetString(), v->GetStringLength());
                                      }
                                    } });
        break;

      default:
        throw NException("Type not supported in Json Reader");
      }
    }
  }

  ~JsonRow() = default;

public:
  virtual inline bool hasTime() const noexcept override {
    return hasTime_;
  }

  // parse a buffer with size into a reset row, call reset before passing row
  virtual bool parse(void* buf, size_t size, nebula::memory::FlatRow& row) noexcept override {
    // can not be a valid json if content smaller than 2
    if (size < 2) {
      return false;
    }

    auto ptr = static_cast<char*>(buf);

    // (Worth A Note)
    // we create a new doc object for each row, seems like it's not expensive
    // but we can try make a reusable doc as member and use Clear method to release resources - not tested
    // Previously, putting this as a member cause lots of memory leak as internal buffer not released.
    rapidjson::Document doc;
    auto& parsed = doc.Parse(ptr, size);
    if (parsed.HasParseError()) {
      LOG(WARNING) << "Error parsing json: " << parsed.GetParseError();
      return false;
    }

    if (!doc.IsObject()) {
      LOG(WARNING) << "Unexpected input - Not an JSON object.";
      return false;
    }

    // iterate through all desired name
    for (auto& column : props_) {
      auto& name = column.first;
      auto& prop = column.second;

      // use the pointer to get value pointer by path
      // https://rapidjson.org/md_doc_pointer.html
      rapidjson::Value* node = locate(doc, prop.path);

      // if not found the node or node has null value
      if ((node == nullptr || node->IsNull()) && !defaultNull_) {
        row.writeNull(name);
        continue;
      }

      prop.action(row, name, node);
    }

    return true;
  }

  virtual void nullify(nebula::memory::FlatRow& row, size_t time) noexcept override {
    // write everything a null if encoutering an invalid message
    row.reset();
    if (!hasTime()) {
      row.write(nebula::meta::Table::TIME_COLUMN, time);
    }

    for (auto itr = props_.cbegin(); itr != props_.cend(); ++itr) {
      row.writeNull(itr->first);
    }
  }

private:
  // use default value for null case
  bool defaultNull_;
  // flag to indicate if current schema has time column incldued
  bool hasTime_;

  // column writer lambda
  nebula::common::unordered_map<std::string, ColumnProps> props_;
};

class JsonReader : public nebula::surface::RowCursor {
  static constexpr size_t SLICE_SIZE = 1024;

public:
  JsonReader(const std::string& file, nebula::type::Schema schema, bool nullDefault = true)
    : nebula::surface::RowCursor(0),
      fstream_{ file },
      json_{ schema, nullDefault },
      row_{ SLICE_SIZE } {
    // read first line to initialize cursor state
    if (std::getline(fstream_, line_)) {
      ++size_;
    }
  }
  virtual ~JsonReader() = default;

public:
  // next row data of JsonRow
  virtual const nebula::surface::RowData& next() override {
    index_++;

    // set it up before returning to client to consume
    row_.reset();
    json_.parse(line_.data(), line_.size(), row_);

    // read next row, if true, then it has data
    if (std::getline(fstream_, line_)) {
      ++size_;
    }

    return row_;
  }

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t) const override {
    throw NException("stream-based JSON Reader does not support random access.");
  }

private:
  std::ifstream fstream_;
  JsonRow json_;
  nebula::memory::FlatRow row_;
  std::string line_;
};

} // namespace storage
} // namespace nebula