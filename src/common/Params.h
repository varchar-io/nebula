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

#include <fmt/format.h>
#include <map>
#include <rapidjson/document.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "Hash.h"
#include "common/Wrap.h"

/**
 * Defines an input parameter as key-value list, everything encoded in string.
 * The parameter is used for macro replacement purpose.
 */
namespace nebula {
namespace common {

// shortcut: nebula common map of key-value in string types
using MapKV = std::unordered_map<std::string, std::string>;
// more efficient representation
using InternalMapKV = unordered_map<std::string_view, std::string_view>;
inline InternalMapKV mapKV2(const MapKV& map) {
  InternalMapKV imkv;
  for (const auto& kv : map) {
    imkv.emplace(kv.first, kv.second);
  }
  return imkv;
}

struct Param {
  explicit Param(std::string_view n, std::vector<std::string_view> v)
    : name{ n }, values{ std::move(v) }, cursor{ 0 } {}

  std::string_view name;
  std::vector<std::string_view> values;
  size_t cursor;

  bool atEnd() const {
    return cursor == values.size();
  }

  const std::string_view& current() const {
    return values.at(cursor);
  }
};

// a param list is a list of macros where each of them may have multiple values
// this list provides an interface to provide all combinations to iterate on
class ParamList {
public:
  // construct a param list from key-values directly
  ParamList(const std::map<std::string, std::vector<std::string>>& macros) {
    for (auto& kv : macros) {
      std::vector<std::string_view> values;
      nebula::common::vector_reserve(values, kv.second.size(), fmt::format("ParamList.macros-{0}", kv.first));
      for (auto& v : kv.second) {
        values.emplace_back(v);
      }
      params_.emplace_back(kv.first, values);
    }
  }

  // construct a param list from a json doc, for example:
  // {a: [1, 2, 3], b: ["x", "y"]}
  ParamList(const rapidjson::Document& doc) {
    auto obj = doc.GetObject();
    nebula::common::vector_reserve(params_, obj.MemberCount(), "ParamList.doc");

    for (auto& m : obj) {
      std::vector<std::string_view> values;
      if (m.value.IsArray()) {
        auto a = m.value.GetArray();
        nebula::common::vector_reserve(values, a.Size(), "ParamList.doc.array");
        for (auto& v : a) {
          values.push_back(v.GetString());
        }
      } else {
        values.reserve(1);
        values.push_back(m.value.GetString());
      }

      params_.emplace_back(m.name.GetString(), values);
    }
  }

  std::unordered_map<std::string, std::string> next() {
    std::unordered_map<std::string, std::string> map;
    // we only need to check if first item is out of range
    if (params_.empty() || params_.at(0).atEnd()) {
      return map;
    }

    // snap it
    for (auto& p : params_) {
      map.emplace(p.name, p.current());
    }

    // update state to next state starting from last item
    for (size_t i = params_.size(); i > 0; --i) {
      auto& p = params_.at(i - 1);
      // increase current param
      ++p.cursor;
      if (!p.atEnd()) {
        break;
      }

      // I'm at end, reset myself and increase previous one
      // but we don't reset the first one which is used as indicator of exhaustion
      if (i > 1) {
        p.cursor = 0;
      }
    }

    return map;
  }

private:
  std::vector<Param> params_;
};

} // namespace common
} // namespace nebula