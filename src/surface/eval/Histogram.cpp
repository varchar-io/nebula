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

#include "Histogram.h"

#include <rapidjson/document.h>

#include "common/Errors.h"

namespace nebula {
namespace surface {
namespace eval {

std::shared_ptr<Histogram> from(const std::string& str) {
  rapidjson::Document doc;
  auto& parsed = doc.Parse(str.data(), str.size());
  N_ENSURE(!parsed.HasParseError() && doc.IsObject(), "invalid json for histogram.");

  // populate data into row object
  auto obj = doc.GetObject();
  auto type = obj.FindMember("type");
  N_ENSURE(type != obj.MemberEnd(), "type is required for histogram.");
  std::string_view s(type->value.GetString(), type->value.GetStringLength());
  // 4 types are supported BASE, BOOL, INT, REAL
  if (s == "BASE") {
    return std::make_shared<Histogram>(obj["count"].GetUint64());
  }

  if (s == "BOOL") {
    return std::make_shared<BoolHistogram>(obj["count"].GetUint64(), obj["trues"].GetUint64());
  }

  if (s == "REAL") {
    return std::make_shared<RealHistogram>(
      obj["count"].GetUint64(),
      obj["min"].GetDouble(),
      obj["max"].GetDouble(),
      obj["sum"].GetDouble());
  }

  if (s == "INT") {
    return std::make_shared<IntHistogram>(
      obj["count"].GetUint64(),
      obj["min"].GetInt64(),
      obj["max"].GetInt64(),
      obj["sum"].GetInt64());
  }

  throw NException(fmt::format("Histogram type not supported {0}", s));
}

} // namespace eval
} // namespace surface
} // namespace nebula
