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

#include "NebulaService.h"

/**
 * provide common data operations for service
 */
namespace nebula {
namespace service {

using nebula::surface::RowCursor;
using nebula::surface::RowData;
using nebula::type::Kind;
using nebula::type::Schema;

#define ERROR_MESSSAGE_CASE(ERROR)                 \
  case ErrorCode::ERROR: {                         \
    return ErrorTraits<ErrorCode::ERROR>::MESSAGE; \
  }

const std::string ServiceProperties::errorMessage(ErrorCode error) {
  switch (error) {
    ERROR_MESSSAGE_CASE(NONE)
    ERROR_MESSSAGE_CASE(MISSING_TABLE)
    ERROR_MESSSAGE_CASE(MISSING_TIME_RANGE)
    ERROR_MESSSAGE_CASE(MISSING_OUTPUT_FIELDS)
    ERROR_MESSSAGE_CASE(FAIL_BUILD_QUERY_PLAN)
    ERROR_MESSSAGE_CASE(FAIL_EXECUTE_QUERY)
  default: throw NException("Error Code Not Covered");
  }
}
#undef ERROR_MESSSAGE_CASE

const std::string ServiceProperties::jsonify(const RowCursor data, const Schema schema) {
  // set up JSON writer to serialize each row
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> json(buffer);

  // set up function callback to serialize each row with capture of the JSON writer
  std::vector<std::function<void(const RowData&)>> jsonCalls;
  auto numColumns = schema->size();
  jsonCalls.reserve(numColumns);
  for (size_t i = 0; i < numColumns; ++i) {
    auto type = schema->childType(i);
    const auto& name = type->name();

// define callback case for each type with gluing JSON function and row function
#define CALLBACK_CASE(K, JF, RF)                            \
  case Kind::K: {                                           \
    jsonCalls.push_back([name, &json](const RowData& row) { \
      json.Key(name);                                       \
      json.JF(row.RF(name));                                \
    });                                                     \
    break;                                                  \
  }

    switch (type->k()) {
      CALLBACK_CASE(BOOLEAN, Bool, readBool)
      CALLBACK_CASE(TINYINT, Int, readByte)
      CALLBACK_CASE(SMALLINT, Int, readShort)
      CALLBACK_CASE(INTEGER, Int, readInt)
      CALLBACK_CASE(BIGINT, Int64, readLong)
      CALLBACK_CASE(REAL, Double, readFloat)
      CALLBACK_CASE(DOUBLE, Double, readDouble)
    case Kind::VARCHAR: {
      jsonCalls.push_back([name, &json](const RowData& row) {
        json.Key(name);
        auto sv = row.readString(name);
        json.String(sv.data());
      });
      break;
    }
    default:
      throw NException("Json serialization not supporting this type yet");
    }

#undef CALLBACK_CASE
  }

  // all rows fit in an array
  json.StartArray();
  // serialize every single row
  while (data->hasNext()) {
    const auto& row = data->next();
    json.StartObject();

    for (auto& f : jsonCalls) {
      f(row);
    }

    json.EndObject();
  }

  json.EndArray();
  return buffer.GetString();
}

} // namespace service
} // namespace nebula