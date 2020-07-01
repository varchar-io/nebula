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

#include <fstream>
#include <iostream>
#include <rapidjson/document.h>
#include <string>
#include <unordered_map>
#include "common/Conv.h"

#include "RowParser.h"
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

// represent a reusable row object with single line content
// we can always parse line for a row object
class JsonRow final : public RowParser {
  using fop = std::function<void(nebula::memory::FlatRow&, const std::string&, const rapidjson::Value&)>;

public:
  JsonRow(nebula::type::Schema schema, bool nullDefault = true)
    : schema_{ schema }, nullDefault_{ nullDefault }, hasTime_{ false } {

// define how each column read and write to row object
// if the provided value is string, we use safe_to to convert it to desired type without exception
// other excpetions, we let it throw
#define CASE_POP(K, F)                                                                              \
  case nebula::type::Kind::K: {                                                                     \
    using T = nebula::type::TypeTraits<nebula::type::Kind::K>;                                      \
    func_.emplace(name,                                                                             \
                  [](nebula::memory::FlatRow& r, const std::string& n, const rapidjson::Value& v) { \
                    if (v.IsNull()) {                                                               \
                      r.write(n, nebula::type::TypeDetect<T::CppType>::value);                      \
                    } else if (v.IsString()) {                                                      \
                      r.write(n, nebula::common::safe_to<T::CppType>(v.GetString()));               \
                    } else {                                                                        \
                      r.write(n, (T::CppType)v.F());                                                \
                    }                                                                               \
                  });                                                                               \
    break;                                                                                          \
  }

    for (size_t i = 0; i < schema_->size(); ++i) {
      auto type = schema_->childType(i);
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
        func_.emplace(name,
                      [](nebula::memory::FlatRow& r, const std::string& n, const rapidjson::Value& v) {
                        // is null or is not expected string type (malformed data) - Nebula enforce types.
                        // we have chance to compatible with other types and convert them into string, such as numbers.
                        if (v.IsNull() || !v.IsString()) {
                          r.write(n, "");
                        } else {
                          r.write(n, v.GetString(), v.GetStringLength());
                        }
                      });
        break;

      default:
        throw NException("Type not supported in Json Reader");
      }
    }
  }

  ~JsonRow() = default;

public:
  virtual bool hasTime() const noexcept override {
    return hasTime_;
  }

  // parse a buffer with size into a reset row, call reset before passing row
  virtual bool parse(void* buf, size_t size, nebula::memory::FlatRow& row) noexcept override {
    auto ptr = static_cast<char*>(buf);

    // (Worth A Note)
    // we create a new doc object for each row, seems like it's not expensive
    // but we can try make a reusable doc as member and use Clear method to release resources - not tested
    // Previously, putting this as a member cause lots of memory leak as internal buffer not released.
    rapidjson::Document doc;
    if (doc.Parse(ptr, size).HasParseError()) {
      LOG(WARNING) << "Error parsing json: " << std::string_view(ptr, size);
      return false;
    }

    // populate data into row object
    auto obj = doc.GetObject();
    for (auto& m : obj) {
      auto name = m.name.GetString();
      if (m.value.IsNull() && !nullDefault_) {
        row.writeNull(name);
        continue;
      }

      // look up the populate function to set the value
      // we do search here to support JSON has more fields than client wants
      auto f = func_.find(name);
      if (f != func_.end()) {
        f->second(row, name, m.value);
      }
    }

    return true;
  }

  virtual void nullify(nebula::memory::FlatRow& row) noexcept override {
    // write everything a null if encoutering an invalid message
    row.reset();
    if (!hasTime()) {
      row.write(nebula::meta::Table::TIME_COLUMN, 0l);
    }

    for (auto itr = func_.cbegin(); itr != func_.cend(); ++itr) {
      row.writeNull(itr->first);
    }
  }

private:
  nebula::type::Schema schema_;
  // use default value for null case
  bool nullDefault_;
  // flag to indicate if current schema has time column incldued
  bool hasTime_;

  // column writer lambda
  std::unordered_map<std::string, fop> func_;
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
  // next row data of CsvRow
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