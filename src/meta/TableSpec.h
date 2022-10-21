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
#include <unordered_map>

#include "common/Evidence.h"
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

// Time to live- measured by seconds
struct TTL {
  explicit TTL(size_t seconds = 0)
    : time{ nebula::common::Evidence::unix_timestamp() },
      stl{ seconds } {}
  virtual ~TTL() = default;

  // check if it's already expired
  inline bool expired() const {
    return stl > 0 && (time + stl) < nebula::common::Evidence::unix_timestamp();
  }

  inline bool never() const {
    return stl == 0;
  }

  // reset impression time - expiration extended
  inline void reset() {
    time = nebula::common::Evidence::unix_timestamp();
  }

  // impression time
  size_t time;
  // seconds to live
  size_t stl;

  MSGPACK_DEFINE(time, stl);
};

#define READ_MEMBER(NAME, VAR, GETTER)    \
  {                                       \
    auto member = obj.FindMember(NAME);   \
    if (member != obj.MemberEnd()) {      \
      this->VAR = member->value.GETTER(); \
    }                                     \
  }

struct TimeSpec {
  TimeType type;
  // unix time value if provided
  int64_t unixTimeValue;
  // column name for given
  std::string column;
  // time pattern to parse value out
  // if pattern not given which implies it is a string column
  // the column will be treated as integer of unix time value
  std::string pattern;

  void from(const rapidjson::GenericObject<true, rapidjson::Value>& obj) {
    READ_MEMBER("column", column, GetString);
    READ_MEMBER("pattern", pattern, GetString);

    // convert type
    auto member = obj.FindMember("type");
    if (member != obj.MemberEnd()) {
      this->type = TimeTypeUtils::from(member->value.GetString());
    }
  }

  // make it msgpack serializable
  MSGPACK_DEFINE(type, unixTimeValue, column, pattern);
};

// serde info for some data format, such as thrift
struct KafkaSerde {
  // kafka topic retention of seconds
  uint64_t retention = 0;

  // size of each ingestion batch
  uint64_t size = 0;

  // kafka topic
  std::string topic;

  // make it msgpack serializable
  MSGPACK_DEFINE(retention, size, topic);
};

// serde info for some data format, such as thrift
struct RocksetSerde {
  // interval time for data slice
  uint32_t interval;

  // API key to access the given URL for final data
  std::string apiKey;

  // make it msgpack serializable
  MSGPACK_DEFINE(interval, apiKey);
};

// format related props
struct CsvProps {
  // indicating if first row is header
  bool hasHeader;
  // indicating if second row is meta
  bool hasMeta;
  // only first letter is used
  std::string delimiter;
  // compression method: gz, ccsv, <empty>
  std::string compression;

  CsvProps(bool h = true,
           bool m = false,
           std::string d = ",",
           std::string c = "")
    : hasHeader{ h },
      hasMeta{ m },
      delimiter{ std::move(d) },
      compression{ std::move(c) } {}

  // materialize csv props from json settings
  void from(const rapidjson::GenericObject<true, rapidjson::Value>& obj) {
    READ_MEMBER("hasHeader", hasHeader, GetBool);
    READ_MEMBER("hasMeta", hasMeta, GetBool);
    READ_MEMBER("delimiter", delimiter, GetString);
    READ_MEMBER("compression", compression, GetString);
  }

  // make it msgpack serializable
  MSGPACK_DEFINE(hasHeader, hasMeta, delimiter, compression);
};

struct JsonProps {
  // rows field, given a json object
  // empty("") means it is just a row object
  // "[ROOT]" means it is an array, every item is a row object
  // other value will navigate the object to get the array of rows
  std::string rowsField;

  // column name to row field mapping - kinda of renaming/alias support
  std::unordered_map<std::string, std::string> columnsMap;

  // materialize json props from json settings
  void from(const rapidjson::GenericObject<true, rapidjson::Value>& obj) {
    READ_MEMBER("rowsField", rowsField, GetString)
    // populate the map from json object with string to string map
    auto cm = obj.FindMember("columnsMap");
    if (cm != obj.MemberEnd()) {
      const auto& map = cm->value.GetObject();
      for (auto& prop : map)
        this->columnsMap[prop.name.GetString()] = prop.value.GetString();
    }
  }

  // make it msgpack serializable
  MSGPACK_DEFINE(rowsField, columnsMap);
};

struct ThriftProps {
  // protocol - thrift has binary, or compact protocol
  std::string protocol;

  // column map from column name to field ID
  // which should be defined by thrift schema
  std::unordered_map<std::string, uint32_t> columnsMap;

  // make it msgpack serializable
  MSGPACK_DEFINE(protocol, columnsMap);
};

struct TableSpec;
// define table spec pointer
using TableSpecPtr = std::shared_ptr<TableSpec>;

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
  // data format: csv, json, thrift, parquet
  DataFormat format;
  // format dependant property for csv
  CsvProps csv;
  // format dependant property for json
  JsonProps json;
  // format dependant property for thrift
  ThriftProps thrift;
  // Serde of the data
  KafkaSerde kafkaSerde;
  // serde of rockset data
  RocksetSerde rocksetSerde;
  // column properties
  ColumnProps columnProps;
  // time spec to generate time value
  TimeSpec timeSpec;
  // access spec
  AccessSpec accessSpec;
  // bucket info
  BucketInfo bucketInfo;
  // settings spec just get list of key-values
  nebula::type::Settings settings;
  // user defined macros
  std::map<std::string, std::vector<std::string>> macroValues;
  // user specified headers - implicit for http but applicable to any service.
  std::vector<std::string> headers;
  // optimal block size, used to combine files into blocks that are as close to this size as possible without going over
  size_t optimalBlockSize;
  // unix time in seconds to indicate when the table expires for ephemeral table only, use 0 for never expire
  nebula::meta::TTL ttl;

  explicit TableSpec() : ttl{ 0 } {}
  explicit TableSpec(std::string _name, size_t maxMb, size_t maxSeconds, std::string _schema,
                     DataSource ds, std::string _loader, std::string _location, std::string _backup,
                     DataFormat _format, CsvProps csvProps, JsonProps jsonProps, ThriftProps thriftProps,
                     KafkaSerde _kafkaSerde, RocksetSerde _rocksetSerde,
                     ColumnProps _columnProps, TimeSpec _timeSpec,
                     AccessSpec _accessSpec, BucketInfo _bucketInfo, nebula::type::Settings _settings,
                     std::map<std::string, std::vector<std::string>> _macroValues, std::vector<std::string> _headers,
                     size_t _optimalBlockSize)
    : name{ std::move(_name) },
      max_mb{ maxMb },
      max_seconds{ maxSeconds },
      schema{ std::move(_schema) },
      source{ ds },
      loader{ std::move(_loader) },
      location{ std::move(_location) },
      backup{ std::move(_backup) },
      format{ _format },
      csv{ std::move(csvProps) },
      json{ std::move(jsonProps) },
      thrift{ std::move(thriftProps) },
      kafkaSerde{ std::move(_kafkaSerde) },
      rocksetSerde{ std::move(_rocksetSerde) },
      columnProps{ std::move(_columnProps) },
      timeSpec{ std::move(_timeSpec) },
      accessSpec{ std::move(_accessSpec) },
      bucketInfo{ std::move(_bucketInfo) },
      settings{ std::move(_settings) },
      macroValues{ std::move(_macroValues) },
      headers{ std::move(_headers) },
      optimalBlockSize{ _optimalBlockSize },
      ttl{ 0 } {}

  // make it msgpack serializable
  MSGPACK_DEFINE(name, max_mb, max_seconds, schema,
                 source, loader, location, backup, format,
                 csv, json, thrift, kafkaSerde, rocksetSerde,
                 columnProps, timeSpec, accessSpec, bucketInfo, settings,
                 macroValues, headers, ttl);

  inline std::string toString() const {
    // table name @ location - format: time
    return fmt::format("{0}@{1}-{2}: {3}", name, location, format, timeSpec.unixTimeValue);
  }

  // generate table pointer which only cares by internal storage/engine
  // NOTE: a bit more on why we have TableSpec and Table.
  // TableSpec is a verbose info that interfaced with end user to define everything regards the table.
  // Table is however only cared by internal storage/engine for data store and query serving.
  std::shared_ptr<Table> to() const {
    // raw schema to manipulate on
    auto schemaPtr = nebula::type::TypeSerializer::from(schema);

    // we need a time column for any input data source
    schemaPtr->addChild(nebula::type::LongType::createTree(Table::TIME_COLUMN));

    // if time column is provided by input data, we will remove it for final schema
    if (timeSpec.type == TimeType::COLUMN) {
      schemaPtr->remove(timeSpec.column);
    }

    // build up a new table from this spec
    return std::make_shared<Table>(name, schemaPtr, columnProps, accessSpec);
  }

  // serialize a table spec into a string
  static std::string serialize(const TableSpec&) noexcept;
  // deserialize a table spec from a string
  static TableSpecPtr deserialize(const std::string_view);
};

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

#undef READ_MEMBER

} // namespace meta
} // namespace nebula
