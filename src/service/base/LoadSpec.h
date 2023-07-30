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

#include "common/Errors.h"
#include "common/Evidence.h"
#include "common/Format.h"
#include "common/Hash.h"
#include "common/Wrap.h"
#include "meta/TableSpec.h"
#include "storage/NFS.h"

namespace nebula {
namespace service {
namespace base {

// extract more info from a load spec
struct LoadSpec;
void extract(LoadSpec&);

#define READ_MEMBER(NAME, VAR, TC, GETTER)                 \
  {                                                        \
    auto member = obj.FindMember(NAME);                    \
    if (member != obj.MemberEnd() && member->value.TC()) { \
      this->VAR = member->value.GETTER();                  \
    }                                                      \
  }

// client side will send a load spec to load
// the spec definition is parsed from LoadRequest.json in nebula.proto
struct LoadSpec {
  static constexpr auto MAX_SIZE_MB = 10240;
  static constexpr auto BACKUP = "";
  static constexpr auto LOADER = "Api";

  // data path
  std::string path;

  // data source
  nebula::meta::DataSource source;

  // data schema - we will do auto detect if missing
  std::string schema;
  std::string domain;

  // data format - such as "csv"
  std::string format;

  // csv props
  meta::CsvProps csv;

  // column props
  meta::ColumnProps props;

  // json props
  meta::JsonProps json;

  // thrift props
  meta::ThriftProps thrift;

  // optional access token
  std::string token;

  // time spec of this load demand
  nebula::meta::TimeSpec timeSpec;

  // macros used to materialize path
  std::map<std::string, std::vector<std::string>> macros;

  // access spec of this sheet - only accessible to current user
  // or user can grant access to specified groups
  // TODO(cao): every user will have a special group [<user>] to set access to him/her only
  nebula::meta::AccessSpec accessSpec;

  // settings
  nebula::type::Settings settings;

  // headers
  std::vector<std::string> headers;

  size_t optimalBlockSize = 0;

  // construct from the json object
  explicit LoadSpec(const rapidjson::Document& doc) {
    // root object
    auto obj = doc.GetObject();

    // object example:
    // {
    //     "path":      "s3://pinlogs/nebula/datahub/result_2216307_2746557.csv",
    //     "schema":    "ROW<name:string, vcpu:int, memory:int, cost_per_hour:bigint>"
    //     "format":    "csv"
    //     "time":      { "column": "col-1", "pattern": "%m/%d/%Y %H:%M:%S" }
    // }
    READ_MEMBER("path", path, IsString, GetString)
    READ_MEMBER("schema", schema, IsString, GetString)
    READ_MEMBER("format", format, IsString, GetString)
    READ_MEMBER("token", token, IsString, GetString)

    N_ENSURE(path.size() > 0, "data path is required");
    N_ENSURE(schema.size() > 0, "data schema is required");
    N_ENSURE(format.size() > 0, "data format is required");

    // extract other info such as source, domain, etc.
    extract(*this);

    // read column options if present
    {
      auto member = obj.FindMember("options");
      if (member != obj.MemberEnd() && member->value.IsObject()) {
        const auto& optionsObj = member->value.GetObject();
        for (auto& prop : optionsObj) {
          const auto& colName = prop.name.GetString();
          const auto& options = prop.value.GetObject();
          auto bloom = false;
          auto dict = false;

          member = options.FindMember("bloom");
          if (member != options.MemberEnd()) {
            bloom = member->value.GetBool();
          }

          member = options.FindMember("dict");
          if (member != options.MemberEnd()) {
            dict = member->value.GetBool();
          }

          // add this
          this->props.emplace(colName, nebula::meta::Column{ bloom, dict });
        }
      }
    }
    // read csv props from the object
    {
      auto member = obj.FindMember("csv");
      if (member != obj.MemberEnd() && member->value.IsObject()) {
        const auto& csvObj = member->value.GetObject();
        this->csv.from(csvObj);
      }
    }
    // read json props
    {
      auto member = obj.FindMember("json");
      if (member != obj.MemberEnd() && member->value.IsObject()) {
        const auto& jsonObj = member->value.GetObject();
        this->json.from(jsonObj);
      }
    }
    // read macros
    {
      auto member = obj.FindMember("macros");
      if (member != obj.MemberEnd() && member->value.IsObject()) {
        const auto& macroObj = member->value.GetObject();
        for (auto& prop : macroObj) {
          const std::string name = prop.name.GetString();
          const auto& value = prop.value;
          if (!name.empty() && value.IsArray()) {
            std::vector<std::string> macroValues;
            const auto& values = value.GetArray();
            const auto size = values.Size();
            nebula::common::vector_reserve(macroValues, size, fmt::format("LoadSpec::macros: {0}", name));
            for (size_t i = 0; i < size; ++i) {
              macroValues.emplace_back(values[i].GetString());
            }

            this->macros.emplace(std::move(name), std::move(macroValues));
          }
        }
      }
    }

    // time definition
    {
      auto member = obj.FindMember("time");
      if (member != obj.MemberEnd() && member->value.IsObject()) {
        const auto& timeObj = member->value.GetObject();
        this->timeSpec.from(timeObj);
      } else {
        // if time is not defined, use a static current time as its time
        this->timeSpec.type = nebula::meta::TimeType::STATIC;
        this->timeSpec.unixTimeValue = nebula::common::Evidence::unix_timestamp();
      }
    }

    // set access token if present through settings
    if (!token.empty()) {
      settings["token"] = token;
    }

    // headers definition
    {
      auto member = obj.FindMember("headers");
      if (member != obj.MemberEnd() && member->value.IsArray()) {
        const auto& list = member->value.GetArray();
        for (size_t i = 0; i < list.Size(); ++i) {
          headers.emplace_back(list[i].GetString());
        }
      }
    }

    // optimal block size definition
    {
      auto member = obj.FindMember("optimal-block-size");
      if (member != obj.MemberEnd() && member->value.IsInt()) {
        optimalBlockSize = member->value.GetInt();
      }
    }
  }
};

// extract other information from given path
void extract(LoadSpec& spec) {
  // detect data source
  auto uri = nebula::storage::parse(spec.path);
  const auto ds = nebula::meta::DataSourceUtils::from(uri.schema);

  // custom data source is not allowed to be visible to external (API)
  if (ds != nebula::meta::DataSource::NEBULA) {
    spec.source = ds;
    spec.domain = uri.host;

    // for file system, update the path to path (key, prefix)
    if (nebula::meta::DataSourceUtils::isFileSystem(ds)) {
      spec.path = uri.path;
    }
  }
}

#undef READ_MEMBER

} // namespace base
} // namespace service
} // namespace nebula