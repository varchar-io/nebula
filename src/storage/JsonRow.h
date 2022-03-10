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

#include <rapidjson/document.h>
#include <rapidjson/pointer.h>

#include "RowParser.h"
#include "common/Chars.h"
#include "common/Conv.h"
#include "common/Errors.h"
#include "memory/FlatRow.h"
#include "meta/TableSpec.h"

// define a json row to read data from a json object string
// or a parsed json object using rapidjson parser
namespace nebula {
namespace storage {

// rapidjson provides a quick way to locate a node if providing a path like `/node-a/node-b`
inline rapidjson::Value* locate(rapidjson::GenericObject<false, rapidjson::Value>& parent,
                                const std::string& path) {
  return rapidjson::Pointer(path.c_str()).Get(parent);
}

// define column properties: column name to path mapping, and action to read value
using fop = std::function<void(nebula::memory::FlatRow&, const std::string&, const rapidjson::Value*)>;
struct JsonColumn {
  // maybe empty - but storing parsed path if the name maps to a path
  std::string path;

  // action to convert data from json node to given storage (eg. FlatRow)
  fop action;
};

// define how each column read and write to row object
// if the provided value is string, we use safe_to to convert it to desired type without exception
// other excpetions, we let it throw
#define CASE_POP(K, C, F)                                                                                          \
  case nebula::type::Kind::K: {                                                                                    \
    using T = nebula::type::TypeTraits<nebula::type::Kind::K>;                                                     \
    columns_.emplace(name,                                                                                         \
                     JsonColumn{ nebula::common::Chars::path(path.data(), path.size()),                            \
                                 [](nebula::memory::FlatRow& r, const std::string& n, const rapidjson::Value* v) { \
                                   if (v == nullptr || v->IsNull()) {                                              \
                                     r.write(n, nebula::type::TypeDetect<T::CppType>::value);                      \
                                   } else if (v->IsString()) {                                                     \
                                     r.write(n, nebula::common::safe_to<T::CppType>(v->GetString()));              \
                                   } else if (v->C()) {                                                            \
                                     r.write(n, (T::CppType)v->F());                                               \
                                   } else {                                                                        \
                                     r.write(n, static_cast<T::CppType>(v->GetDouble()));                          \
                                   }                                                                               \
                                 } });                                                                             \
    break;                                                                                                         \
  }

// represent a reusable row object with single line content
// we can always parse line for a row object
class JsonRow final : public RowParser {
public:
  // to support flat data from nested structure in JSON, introduce "pathInName" variable
  // if this is true, use name as path to match value otherwise use name itself
  // currently we default to support path in name.
  JsonRow(nebula::type::Schema schema,
          std::unordered_map<std::string, std::string> columnsMap = {},
          const std::vector<std::string>& columns = {},
          bool defaultNull = false)
    : defaultNull_{ defaultNull }, hasTime_{ false } {

    for (size_t i = 0; i < schema->size(); ++i) {
      auto type = schema->childType(i);
      const auto& name = type->name();

      // ignore the column that is not existing in the valid set
      if (!columns.empty() && std::find(columns.begin(), columns.end(), name) == columns.end()) {
        continue;
      }

      if (name == nebula::meta::Table::TIME_COLUMN) {
        hasTime_ = true;
      }

      // if the name has mapped path (a.b)
      auto path = name;
      if (columnsMap.size() > 0) {
        // we don't use direct find, instead case insensitive
        // auto found = columnsMap.find(name);
        auto found = std::find_if(columnsMap.begin(),
                                  columnsMap.end(),
                                  [&name](const auto& vt) {
                                    return nebula::common::Chars::same(vt.first, name);
                                  });
        if (found != columnsMap.end()) {
          path = found->second;
          LOG(INFO) << "path=" << path << " for name=" << name;
        }
      }

      switch (type->k()) {
        CASE_POP(BOOLEAN, IsBool, GetBool)
        CASE_POP(TINYINT, IsInt, GetInt)
        CASE_POP(SMALLINT, IsInt, GetInt)
        CASE_POP(INTEGER, IsInt, GetInt)
        CASE_POP(BIGINT, IsInt64, GetInt64)
        CASE_POP(REAL, IsFloat, GetFloat)
        CASE_POP(DOUBLE, IsDouble, GetDouble)
      case nebula::type::Kind::VARCHAR:
        columns_.emplace(name,
                         JsonColumn{ nebula::common::Chars::path(path.data(), path.size()),
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

    // fill the row object with json doc
    auto root = doc.GetObject();
    fill(root, row);

    return true;
  }

  // fill a flat row with a json object
  void fill(rapidjson::GenericObject<false, rapidjson::Value>& json, nebula::memory::FlatRow& row) const noexcept {
    // iterate through all desired name
    for (auto& column : columns_) {
      auto& name = column.first;
      auto& prop = column.second;

      // use the pointer to get value pointer by path
      // https://rapidjson.org/md_doc_pointer.html
      const rapidjson::Value* node = locate(json, prop.path);

      // if not found the node or node has null value
      if ((node == nullptr || node->IsNull()) && !defaultNull_) {
        row.writeNull(name);
        continue;
      }

      prop.action(row, name, node);
    }
  }

  virtual void nullify(nebula::memory::FlatRow& row, size_t time) noexcept override {
    // write everything a null if encoutering an invalid message
    row.reset();
    if (!hasTime()) {
      row.write(nebula::meta::Table::TIME_COLUMN, time);
    }

    for (auto itr = columns_.cbegin(); itr != columns_.cend(); ++itr) {
      row.writeNull(itr->first);
    }
  }

private:
  // use default value for null case
  bool defaultNull_;
  // flag to indicate if current schema has time column incldued
  bool hasTime_;

  // column writer lambda
  nebula::common::unordered_map<std::string, JsonColumn> columns_;
};

#undef CASE_POP

} // namespace storage
} // namespace nebula