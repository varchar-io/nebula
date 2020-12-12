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

#include <unordered_map>

#include "meta/Table.h"
#include "type/Serde.h"
#include "type/Type.h"

/**
 * Define nebula table and system metadata 
 * which manages what data segments are loaded in memory for each table
 * This meta data can persist and sync with external DB system such as MYSQL or RocksDB
 * (A KV store is necessary for Nebula to manage all metadata)
 * 
 * (Also - Is this responsibility of zookeeper?)
 */
namespace nebula {
namespace meta {

// define data source
enum class DataSource {
  Custom,
  S3,
  LOCAL,
  KAFKA,
  GSHEET
};

struct DataSourceUtils {
  static bool isFileSystem(const DataSource& ds) {
    return ds == DataSource::S3 || ds == DataSource::LOCAL;
  }

  static const std::string& getProtocol(const DataSource& ds) {
    static const std::string NONE = "";
    static const nebula::common::unordered_map<DataSource, std::string> SOURCE_PROTO = {
      { DataSource::S3, "s3" },
      { DataSource::LOCAL, "local" }
    };

    auto p = SOURCE_PROTO.find(ds);
    if (p != SOURCE_PROTO.end()) {
      return p->second;
    }

    return NONE;
  }
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
  PROVIDED
};

// type of macros accepted in table spec
enum class PatternMacro {
  // Daily partition /dt=?
  DAILY,
  // hourly partition name /dt=?/hr=?
  HOURLY,
  // minute partition name /dt=?/hr=?/mi=?
  MINUTELY,
  // use second level directory name /dt=?/hr=?/mi=?/se=?
  SECONDLY,
  // use directory name in unix timestamp /ts=?
  TIMESTAMP,
  // placeholder for not accepted marcos
  INVALID,
};

struct TimeSpec {
  TimeType type;
  // unix time value if provided
  size_t unixTimeValue;
  // column name for given
  std::string colName;
  // time pattern to parse value out
  // if pattern not given which implies it is a string column
  // the column will be treated as integer of unix time value
  std::string pattern;
};

// serde info for some data format, such as thrift
struct KafkaSerde {
  // kafka topic retention of seconds
  uint64_t retention = 0;

  // size of each ingestion batch
  uint64_t size = 0;

  // protocol - thrift has binary, or compact protocol
  // json may have bson variant
  std::string protocol;

  // column map from column name to field ID
  // which should be defined by thrift schema
  nebula::common::unordered_map<std::string, uint32_t> cmap;
};

// TODO(cao): use nebula::common::unordered_map if it supports msgpack serde
// key-value settings in both string types
using Settings = std::unordered_map<std::string, std::string>;

struct TableSpec {
  // table name
  std::string name;
  // max size in MB resident in memory
  size_t max_mb;
  // max time span in secods resident in memory
  size_t max_seconds;
  // table schema
  std::string schema;
  // data source to load from
  DataSource source;
  // loader to decide how to load data in
  std::string loader;
  // source location uri
  std::string location;
  // backup location uri
  std::string backup;
  // data format
  std::string format;
  // Serde of the data
  KafkaSerde serde;
  // column properties
  ColumnProps columnProps;
  // time spec to generate time value
  TimeSpec timeSpec;
  // access spec
  AccessSpec accessSpec;
  // bucket info
  BucketInfo bucketInfo;
  // settings spec just get list of key-values
  Settings settings;

  TableSpec(std::string n, size_t mm, size_t ms, std::string s,
            DataSource ds, std::string lo, std::string loc, std::string bak,
            std::string f, KafkaSerde sd, ColumnProps cp, TimeSpec ts,
            AccessSpec as, BucketInfo bi, Settings st)
    : name{ std::move(n) },
      max_mb{ mm },
      max_seconds{ ms },
      schema{ std::move(s) },
      source{ ds },
      loader{ std::move(lo) },
      location{ std::move(loc) },
      backup{ std::move(bak) },
      format{ std::move(f) },
      serde{ std::move(sd) },
      columnProps{ std::move(cp) },
      timeSpec{ std::move(ts) },
      accessSpec{ std::move(as) },
      bucketInfo{ std::move(bi) },
      settings{ std::move(st) } {}

  inline std::string toString() const {
    // table name @ location - format: time
    return fmt::format("{0}@{1}-{2}: {3}", name, location, format, timeSpec.unixTimeValue);
  }

  // generate table pointer
  std::shared_ptr<Table> to() const {
    // raw schema to manipulate on
    auto schemaPtr = nebula::type::TypeSerializer::from(schema);

    // we need a time column for any input data source
    schemaPtr->addChild(nebula::type::LongType::createTree(Table::TIME_COLUMN));

    // if time column is provided by input data, we will remove it for final schema
    if (timeSpec.type == TimeType::COLUMN) {
      schemaPtr->remove(timeSpec.colName);
    }

    // build up a new table from this spec
    return std::make_shared<Table>(name, schemaPtr, columnProps, accessSpec);
  }
};

// define table spec pointer
using TableSpecPtr = std::shared_ptr<TableSpec>;

// Current hash and equal are based on table name only
// There should not be duplicate table names in the system
struct TableSpecHash {
public:
  size_t operator()(const TableSpecPtr& ts) const {
    return nebula::common::Hasher::hashString(ts->name);
  }
};

struct TableSpecEqual {
public:
  bool operator()(const TableSpecPtr& ts1, const TableSpecPtr& ts2) const {
    return ts1->name == ts2->name;
  }
};

using TableSpecSet = nebula::common::unordered_set<TableSpecPtr, TableSpecHash, TableSpecEqual>;

constexpr auto HOUR_MINUTES = 60;
constexpr auto MINUTE_SECONDS = 60;
constexpr auto DAY_HOURS = 24;
constexpr auto HOUR_SECONDS = HOUR_MINUTES * MINUTE_SECONDS;
constexpr auto DAY_SECONDS = HOUR_SECONDS * DAY_HOURS;

const nebula::common::unordered_map<nebula::meta::PatternMacro, std::string> patternDefinitionStr{
  { nebula::meta::PatternMacro::DAILY, "daily" },
  { nebula::meta::PatternMacro::HOURLY, "hourly" },
  { nebula::meta::PatternMacro::MINUTELY, "minutely" },
  { nebula::meta::PatternMacro::SECONDLY, "secondly" },
  { nebula::meta::PatternMacro::TIMESTAMP, "timestamp" }
};

// match fs location macro e.g {date}
const nebula::common::unordered_map<nebula::meta::PatternMacro, std::string> patternMacroStr{
  { nebula::meta::PatternMacro::DAILY, "date" },
  { nebula::meta::PatternMacro::HOURLY, "hour" },
  { nebula::meta::PatternMacro::MINUTELY, "minute" },
  { nebula::meta::PatternMacro::SECONDLY, "second" },
  { nebula::meta::PatternMacro::TIMESTAMP, "timestamp" }
};

const nebula::common::unordered_map<nebula::meta::PatternMacro, nebula::meta::PatternMacro> childPattern{
  { nebula::meta::PatternMacro::DAILY, nebula::meta::PatternMacro::HOURLY },
  { nebula::meta::PatternMacro::HOURLY, nebula::meta::PatternMacro::MINUTELY },
  { nebula::meta::PatternMacro::MINUTELY, nebula::meta::PatternMacro::SECONDLY }
};

const nebula::common::unordered_map<nebula::meta::PatternMacro, int> unitInSeconds{
  { nebula::meta::PatternMacro::DAILY, DAY_SECONDS },
  { nebula::meta::PatternMacro::HOURLY, HOUR_SECONDS },
  { nebula::meta::PatternMacro::MINUTELY, MINUTE_SECONDS }
};

const nebula::common::unordered_map<nebula::meta::PatternMacro, int> childSize{
  { nebula::meta::PatternMacro::DAILY, DAY_HOURS },
  { nebula::meta::PatternMacro::HOURLY, HOUR_MINUTES },
  { nebula::meta::PatternMacro::MINUTELY, MINUTE_SECONDS }
};

// check if pattern string type
inline nebula::meta::PatternMacro extractPatternMacro(const std::string& pattern) {
  // lowercase pattern string match
  std::string lpattern;
  transform(pattern.begin(), pattern.end(), std::back_inserter(lpattern), tolower);

  const auto tsMacroFound = lpattern.find(patternDefinitionStr.at(PatternMacro::TIMESTAMP)) != std::string::npos;
  const auto dateMacroFound = lpattern.find(patternDefinitionStr.at(PatternMacro::DAILY)) != std::string::npos;
  const auto hourMacroFound = lpattern.find(patternDefinitionStr.at(PatternMacro::HOURLY)) != std::string::npos;
  const auto minuteMacroFound = lpattern.find(patternDefinitionStr.at(PatternMacro::MINUTELY)) != std::string::npos;
  const auto secondMacroFound = lpattern.find(patternDefinitionStr.at(PatternMacro::SECONDLY)) != std::string::npos;

  if (secondMacroFound) {
    return PatternMacro::SECONDLY;
  } else if (minuteMacroFound) {
    return PatternMacro::MINUTELY;
  } else if (hourMacroFound) {
    return PatternMacro::HOURLY;
  } else if (dateMacroFound) {
    return PatternMacro::DAILY;
  }

  if (tsMacroFound && !secondMacroFound && !minuteMacroFound && !hourMacroFound && !dateMacroFound) {
    return PatternMacro::TIMESTAMP;
  }

  return nebula::meta::PatternMacro::INVALID;
}
} // namespace meta
} // namespace nebula