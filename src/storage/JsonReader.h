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

#include <folly/Conv.h>
#include <folly/String.h>
#include <fstream>
#include <iostream>
#include <rapidjson/document.h>
#include <string>
#include <unordered_map>

#include "common/Errors.h"
#include "memory/FlatRow.h"
#include "surface/DataSurface.h"

/**
 * A JSON file reader, the expected file is list of json objects separated by new line.
 * It reads line by line as string, and let JSON parse each line as object
 */
namespace nebula {
namespace storage {

class JsonReader : public nebula::surface::RowCursor {
  static constexpr size_t SLICE_SIZE = 1024;
  using fop = std::function<void(nebula::memory::FlatRow&, const std::string&, const rapidjson::Value&)>;

public:
  JsonReader(const std::string& file, nebula::type::Schema schema, bool nullDefault = true)
    : nebula::surface::RowCursor(0),
      fstream_{ file },
      schema_{ schema },
      nullDefault_{ nullDefault },
      row_{ SLICE_SIZE } {
// define how each column read and write to row object
// if the provided value is string, we use folly::to to convert it to desired type
// other excpetions, we let it throw
#define CASE_POP(K, F)                                                                              \
  case nebula::type::Kind::K: {                                                                     \
    using T = nebula::type::TypeTraits<nebula::type::Kind::K>;                                      \
    func_.emplace(type->name(),                                                                     \
                  [](nebula::memory::FlatRow& r, const std::string& n, const rapidjson::Value& v) { \
                    if (v.IsNull()) {                                                               \
                      r.write(n, nebula::type::TypeDetect<T::CppType>::value);                      \
                    } else if (v.IsString()) {                                                      \
                      r.write(n, folly::to<T::CppType>(v.GetString()));                             \
                    } else {                                                                        \
                      r.write(n, (T::CppType)v.F());                                                \
                    }                                                                               \
                  });                                                                               \
    break;                                                                                          \
  }

    for (size_t i = 0; i < schema_->size(); ++i) {
      auto type = schema_->childType(i);
      switch (type->k()) {
        CASE_POP(BOOLEAN, GetBool)
        CASE_POP(TINYINT, GetInt)
        CASE_POP(SMALLINT, GetInt)
        CASE_POP(INTEGER, GetInt)
        CASE_POP(BIGINT, GetInt64)
        CASE_POP(REAL, GetFloat)
        CASE_POP(DOUBLE, GetDouble)
      case nebula::type::Kind::VARCHAR:
        func_.emplace(type->name(),
                      [](nebula::memory::FlatRow& r, const std::string& n, const rapidjson::Value& v) {
                        if (v.IsNull()) {
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
    readData();

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
  const nebula::surface::RowData& readData() {
    if (doc_.Parse(line_.c_str()).HasParseError()) {
      // TODO(cao) - throw in invalid json line or provide option to skip it?
      throw NException(fmt::format("Error parsing json: {0}.", line_));
    }

    // populate data into row_ object
    row_.reset();
    auto obj = doc_.GetObject();
    for (auto& m : obj) {
      auto name = m.name.GetString();
      if (m.value.IsNull() && !nullDefault_) {
        row_.writeNull(name);
        continue;
      }

      // look up the populate function to set the value
      // we do search here to support JSON has more fields than client wants
      auto f = func_.find(name);
      if (f != func_.end()) {
        f->second(row_, name, m.value);
      }
    }

    return row_;
  }

private:
  std::ifstream fstream_;
  nebula::type::Schema schema_;
  // use default value for null case
  bool nullDefault_;

  // state objects of the reader
  rapidjson::Document doc_;
  std::string line_;
  nebula::memory::FlatRow row_;
  // column writer lambda
  std::unordered_map<std::string, fop> func_;
};

} // namespace storage
} // namespace nebula