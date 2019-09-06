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

#include <unordered_set>

#include "common/Hash.h"
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
  S3
};

// type of time source to fill time column
enum class TimeType {
  // fixed value
  STATIC,
  // using current timestamp when loading
  CURRENT,
  // time is from a given column
  COLUMN
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

struct TableSpec {
  // table name
  std::string name;
  // max size in MB resident in memory
  size_t max_mb;
  // max time span in hour resident in memory
  size_t max_hr;
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
  // time spec to generate time value
  TimeSpec timeSpec;

  TableSpec(std::string n, size_t mm, size_t mh, std::string s,
            DataSource ds, std::string lo, std::string loc, std::string bak, std::string f, TimeSpec ts)
    : name{ std::move(n) },
      max_mb{ mm },
      max_hr{ mh },
      schema{ std::move(s) },
      source{ ds },
      loader{ std::move(lo) },
      location{ std::move(loc) },
      backup{ std::move(bak) },
      format{ std::move(f) },
      timeSpec{ std::move(ts) } {}

  inline std::string toString() const {
    // table name @ location - format: time
    return fmt::format("{0}@{1}-{2}: {3}", name, location, format, timeSpec.unixTimeValue);
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

using TableSpecSet = std::unordered_set<TableSpecPtr, TableSpecHash, TableSpecEqual>;

} // namespace meta
} // namespace nebula