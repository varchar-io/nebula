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

#include <msgpack.hpp>

#include "common/Chars.h"
#include "type/Type.h"

// the major definitions in meta are needed for serialization as they are metadata
// we use msgpack for efficiency.
// group most of the base types definitions here to work with its adapters
namespace nebula {
namespace meta {
// node states
enum class NState {
  ACTIVE,
  BAD,
  DEAD
};

// node role
enum class NRole {
  NODE,
  SERVER
};
// spec state defines states for life cycle of given spec
enum class SpecState : char {
  // NEW spec requires data sync
  NEW = 'N',

  // data of the spec loaded in nebula
  READY = 'A',

  // Spec is waiting for offload
  EXPIRED = 'E',

  // spec is offline - we should have an external location for it
  OFFLINE = 'O',
};

// define data sources supported in Nebula:
// NEBULA is a reserved type only used internally.
// Any external reference will be treated as illegal (invalid)
// Basically the same as each provider's protocol, s3://, gs://, abfs:// etc
// S3: AWS S3, GS: Google cloud storage,
enum class DataSource {
  NEBULA,
  S3,
  GS,
  ABFS,
  LOCAL,
  KAFKA,
  GSHEET,
  HTTP,
  ROCKSET
};

// type of time source to fill time column
enum class TimeType {
  // fixed value
  STATIC,
  // using current timestamp when loading
  CURRENT,
  // time is from a given column
  COLUMN,
  // system defined macro named by pattern
  MACRO,
  // system will provide depending on sub-system behavior
  // such as, Kafka will fill message timestamp for it
  PROVIDED,
  // unkown type - not set
  UNKNOWN,
};

// TODO(cao): define where a rule is applied.
// READ (including UDF), aggregation (UDAF), WRITE
enum class AccessType {
  UNKNOWN,
  READ,
  AGGREGATION,
  WRITE
};

// TODO(cao): action type is generic but here it means more about "reject"
// it defines when rules are not met, how to reject request, mask data or fail the query.
enum ActionType {
  PASS = 0,
  MASK = 1,
  DENY = 2
};

enum class DataFormat {
  CSV,
  JSON,
  THRIFT,
  PARQUET,
  GSHEET,
  UNKNOWN
};

// given variable str
#define SAME_STR_THEN_ITEM(STR, ITEM)          \
  if (nebula::common::Chars::same(str, STR)) { \
    return ITEM;                               \
  }

struct TimeTypeUtils {
  static TimeType from(const std::string& str) noexcept {
    SAME_STR_THEN_ITEM("static", TimeType::STATIC)
    SAME_STR_THEN_ITEM("current", TimeType::CURRENT)
    SAME_STR_THEN_ITEM("column", TimeType::COLUMN)
    SAME_STR_THEN_ITEM("macro", TimeType::MACRO)
    SAME_STR_THEN_ITEM("provided", TimeType::PROVIDED)

    // everything else - use current time
    return TimeType::UNKNOWN;
  }
};

// method to convert string to format enum
struct DataFormatUtils {
  static DataFormat from(const std::string& str) noexcept {
    SAME_STR_THEN_ITEM("csv", DataFormat::CSV)
    SAME_STR_THEN_ITEM("json", DataFormat::JSON)
    SAME_STR_THEN_ITEM("THRIFT", DataFormat::THRIFT)
    SAME_STR_THEN_ITEM("parquet", DataFormat::PARQUET)
    SAME_STR_THEN_ITEM("gsheet", DataFormat::GSHEET)
    return DataFormat::UNKNOWN;
  }
};

struct DataSourceUtils {
  static bool isFileSystem(const DataSource& ds) {
    return ds == DataSource::S3
           || ds == DataSource::GS
           || ds == DataSource::ABFS
           || ds == DataSource::LOCAL;
  }

  // get protocol of this type of data source
  static const std::string& getProtocol(const DataSource& ds) {
    static const std::string NONE = "";
    static const nebula::common::unordered_map<DataSource, std::string> SOURCE_PROTO = {
      { DataSource::S3, "s3" },
      { DataSource::GS, "gs" },
      { DataSource::ABFS, "abfs" },
      { DataSource::LOCAL, "local" },
      { DataSource::HTTP, "http" }
    };

    auto p = SOURCE_PROTO.find(ds);
    if (p != SOURCE_PROTO.end()) {
      return p->second;
    }

    return NONE;
  }

  // Get data source entity from its name
  static DataSource from(const std::string& str) noexcept {
    SAME_STR_THEN_ITEM("s3", DataSource::S3)
    SAME_STR_THEN_ITEM("abfs", DataSource::ABFS)
    SAME_STR_THEN_ITEM("gs", DataSource::GS)
    SAME_STR_THEN_ITEM("kafka", DataSource::KAFKA)
    SAME_STR_THEN_ITEM("rockset", DataSource::ROCKSET)
    SAME_STR_THEN_ITEM("local", DataSource::LOCAL)
    SAME_STR_THEN_ITEM("http", DataSource::HTTP)
    SAME_STR_THEN_ITEM("https", DataSource::HTTP)
    // CUSTOM usually means a internal type such as Nebula Test data set
    // Not allowed to be used for external data
    // throw NException(fmt::format("Unsupported data source: {0}", data));
    return DataSource::NEBULA;
  }
};

#undef SAME_STR_THEN_ITEM

} // namespace meta
} // namespace nebula

MSGPACK_ADD_ENUM(nebula::meta::DataSource)
MSGPACK_ADD_ENUM(nebula::meta::TimeType)
MSGPACK_ADD_ENUM(nebula::meta::AccessType)
MSGPACK_ADD_ENUM(nebula::meta::ActionType)
MSGPACK_ADD_ENUM(nebula::meta::DataFormat)
MSGPACK_ADD_ENUM(nebula::type::Kind)
MSGPACK_ADD_ENUM(nebula::meta::SpecState)
MSGPACK_ADD_ENUM(nebula::meta::NRole)
MSGPACK_ADD_ENUM(nebula::meta::NState)
