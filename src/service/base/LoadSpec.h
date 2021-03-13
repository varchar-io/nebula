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
#include "meta/TableSpec.h"
#include "storage/NFS.h"

namespace nebula {
namespace service {
namespace base {

// extract more info from a load spec
struct LoadSpec;
void extract(const std::string&, LoadSpec&);

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

  // optional access token
  std::string token;

  // time spec of this load demand
  std::string tcol;
  std::string tpat;
  nebula::meta::TimeSpec timeSpec;

  // access spec of this sheet - only accessible to current user
  // or user can grant access to specified groups
  // TODO(cao): every user will have a special group [<user>] to set access to him/her only
  nebula::meta::AccessSpec accessSpec;

  // settings
  nebula::meta::Settings settings;

  // construct from the json object
  explicit LoadSpec(const rapidjson::Document& doc) {
    N_ENSURE(doc.IsObject(), "json object expected in google sheet spec.");

    // root object
    auto obj = doc.GetObject();

#define READ_MEMBER(NAME, VAR, GETTER)    \
  {                                       \
    auto member = obj.FindMember(NAME);   \
    if (member != obj.MemberEnd()) {      \
      this->VAR = member->value.GETTER(); \
    }                                     \
  }

    // object example:
    // {
    //     "path":      "s3://pinlogs/nebula/datahub/result_2216307_2746557.csv",
    //     "schema":    "ROW<name:string, vcpu:int, memory:int, cost_per_hour:bigint>"
    //     "format":    "csv"
    //     "tcol":      "time"
    //     "tpat":      "%m/%d/%Y %H:%M:%S"
    // }
    READ_MEMBER("path", path, GetString)
    READ_MEMBER("schema", schema, GetString)
    READ_MEMBER("format", format, GetString)
    READ_MEMBER("token", token, GetString)

    // time related
    READ_MEMBER("tcol", tcol, GetString)
    READ_MEMBER("tpat", tpat, GetString)

    N_ENSURE(path.size() > 0, "data path is required");
    N_ENSURE(schema.size() > 0, "data schema is required");
    N_ENSURE(format.size() > 0, "data format is required");

    // extract other info such as source, domain, etc.
    extract(path, *this);

    // set access token if present through settings
    if (!token.empty()) {
      settings["token"] = token;
    }

    if (format == "csv") {
      // overwrite delimeter from default [tab] to comma
      this->settings["csv.delimiter"] = ",";
    }

    // TODO(cao): tcol and tpat will define how to get time column
    if (tcol.length() > 0) {
      this->timeSpec.type = nebula::meta::TimeType::COLUMN;
      this->timeSpec.colName = tcol;
      this->timeSpec.pattern = tpat;
    } else {
      this->timeSpec.type = nebula::meta::TimeType::STATIC;
      this->timeSpec.unixTimeValue = nebula::common::Evidence::unix_timestamp();
    }
  }
};

// extract other information from given path
void extract(const std::string& path, LoadSpec& spec) {
  // detect data source
  auto uri = nebula::storage::parse(path);
  const auto ds = nebula::meta::DataSourceUtils::from(uri.schema);

  // custom data source is not allowed to be visible to external (API)
  if (ds != nebula::meta::DataSource::NEBULA) {
    spec.source = ds;
    spec.domain = uri.host;
    spec.path = uri.path;
  }
}

} // namespace base
} // namespace service
} // namespace nebula