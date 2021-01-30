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

namespace nebula {
namespace service {
namespace base {

// client side will send a google sheet object for nebula to load
// the spec definition is parsed from LoadRequest.json in nebula.proto
struct GoogleSheet {
  // limit max size to 10G for now
  static constexpr auto MAX_SIZE_MB = 10240;
  // google sheet will be loaded by HTTP api
  static constexpr auto LOADER = "Api";
  // configured somewhere for backups of google sheets
  static constexpr auto BACKUP = "";
  // google sheets use columns data format
  static constexpr auto FORMAT = "COLUMNS";
  // serde to understand the data in each formats
  static constexpr auto SERDE = "JSON";
  // URI of the sheet (sheet or drive)
  // ref. DataSource def in TableSpec.h
  static constexpr auto SOURCE = "gsheet";
  // we will use this URL to access google sheets
  // https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
  static constexpr auto GSHEET_URL_GET =
    "https://sheets.googleapis.com/v4/spreadsheets/"
    "{id}/values/{range}?majorDimension={format}&key={key}"
    "&valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=SERIAL_NUMBER";

  // sheet ID
  std::string id;

  // user ID owning this sheet
  std::string uid;

  // google sheet schema
  std::string schema;

  // total number of rows
  size_t rows;

  // total number of columns
  size_t cols;

  // gtoken to read data
  std::string gtoken;

  // api key to access google api
  std::string key;

  // time column name if specified, otherwise using static current time
  std::string tcol;

  // the full google sheet URL
  std::string url;

  // time spec of this google sheet
  nebula::meta::TimeSpec timeSpec;

  // access spec of this sheet - only accessible to current user
  // or user can grant access to specified groups
  nebula::meta::AccessSpec accessSpec;

  // settings
  nebula::meta::Settings settings;

  // construct from the json object
  explicit GoogleSheet(const rapidjson::Document& doc) {
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

    // id
    READ_MEMBER("id", id, GetString)

    // uid
    READ_MEMBER("uid", uid, GetString)

    // schema
    READ_MEMBER("schema", schema, GetString)

    // rows and cols
    READ_MEMBER("rows", rows, GetInt64)
    READ_MEMBER("cols", cols, GetInt64)

    // save token in the settings
    // TODO(cao): let server figure out the token by itself
    // not safe to transfer gtoken over wire.
    READ_MEMBER("gtoken", gtoken, GetString)
    READ_MEMBER("key", key, GetString)

    // check if we have time column set -
    // if yes, only supporting serial number for now
    READ_MEMBER("tcolumn", tcol, GetString)

    // save these as settings
    this->settings["token"] = this->gtoken;
    this->settings["key"] = this->key;

    // TODO(cao): split this into multiple specs when row number is large
    // get range of the sheet => A1:{LAST_COL}{MAX_ROW}
    auto range = fmt::format("A1:{0}{1}",
                             (char)('A' + std::max<size_t>(this->cols - 1, 25)),
                             this->rows);

    // url
    this->url = nebula::common::format(GoogleSheet::GSHEET_URL_GET,
                                       { { "id", id },
                                         { "range", range },
                                         { "format", GoogleSheet::FORMAT },
                                         { "key", key } });

    // TODO(cao): use a static time for now
    if (tcol.length() > 0) {
      this->timeSpec.type = nebula::meta::TimeType::COLUMN;
      this->timeSpec.colName = tcol;
      this->timeSpec.pattern = "SERIAL_NUMBER";
    } else {
      this->timeSpec.type = nebula::meta::TimeType::STATIC;
      this->timeSpec.unixTimeValue = nebula::common::Evidence::unix_timestamp();
    }

#undef READ_MEMBER
  }
};

} // namespace base
} // namespace service
} // namespace nebula