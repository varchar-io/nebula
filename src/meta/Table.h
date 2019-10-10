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

#include "common/Errors.h"
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

using nebula::type::Schema;

/**
 * Define column properties that fetched from meta data system
 */
struct Column {
  explicit Column(bool bf = false, bool d = false, const std::string& dv = "")
    : withBloomFilter{ bf }, withDict{ d }, defaultValue{ dv } {}

  // by default, we don't build bloom filter
  bool withBloomFilter;

  // by default, we turn on dictionay for strings
  bool withDict;

  // specify a default value in string
  // empty means no default value,
  // with saying that, we're not support string type with empty stirng as default value
  std::string defaultValue;
};

using ColumnProps = std::unordered_map<std::string, Column>;

class Table {
public:
  Table(const std::string& name) : Table(name, nullptr) {}
  Table(const std::string& name, Schema schema) : Table(name, schema, {}) {}
  Table(const std::string& name, Schema schema, ColumnProps columns)
    : name_{ name }, schema_{ schema }, columns_{ std::move(columns) } {
    // TODO(cao) - load table properties from meta data service
    loadTable();
  }

  virtual ~Table() = default;

  friend bool operator==(const Table& t1, const Table& t2) noexcept {
    return t1.name_ == t2.name_;
  }

  // select *
  static constexpr auto ALL_COLUMNS = "*";

  // default reserved [time] field in nebula, every table has this field enforced.
  // move this to core/meta to be shared everywhere
  static constexpr auto TIME_COLUMN = "_time_";

  // window column is produced from time window based on windowing algorithm
  static constexpr auto WINDOW_COLUMN = "_window_";

public:
  virtual Schema schema() const {
    N_ENSURE_NOT_NULL(schema_, fmt::format("invalid table not found = {0}", name_));
    return schema_;
  }

  inline std::string name() const {
    return name_;
  }

  // retrieve all column meta data by its name
  virtual Column column(const std::string& col) const noexcept {
    auto column = columns_.find(col);
    if (column != columns_.end()) {
      return column->second;
    }

    return Column{};
  }

protected:
  // table name is global unique, but it can be organized by some namespace style naming convention
  // such as "nebula.test"
  std::string name_;
  Schema schema_;
  ColumnProps columns_;

private:
  void loadTable();
};
} // namespace meta
} // namespace nebula