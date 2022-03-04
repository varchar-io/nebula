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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "meta/TableSpec.h"

/**
 * Test namespace for testing external dependency APIs
 */
namespace nebula {
namespace meta {
namespace test {

TEST(MetaTest, TestTableSpecSerde) {
  CsvProps csvProps{ true, false, "sep" };
  JsonProps jsonProps{ "rows", { { "col1", "field1" } } };
  ThriftProps thriftProps{ "binary", { { "col1", 1 } } };
  KafkaSerde kafkaSerde{ 1, 2, "topic" };
  RocksetSerde rocksetSerde{ 5, "key" };
  ColumnProps columnProps{ { "c1", Column{} } };
  TimeSpec timeSpec{ TimeType::STATIC, 123, "ct", "xyz" };
  AccessSpec accessSpec{
    AccessRule{ AccessType::READ, { "nebula-users" }, ActionType::MASK }
  };
  BucketInfo bucketInfo = BucketInfo::empty();
  std::unordered_map<std::string, std::string> settings{ { "key1", "value1" }, { "key2", "value2" } };
  std::map<std::string, std::vector<std::string>> macroValues{};
  std::vector<std::string> headers;
  TableSpec spec{
    "abc", 1, 2, "ROW<id:bigint>", DataSource::S3,
    "Roll", "s3://nebula", "s3://backup",
    DataFormat::JSON, std::move(csvProps), std::move(jsonProps), std::move(thriftProps),
    std::move(kafkaSerde), std::move(rocksetSerde),
    std::move(columnProps), std::move(timeSpec),
    std::move(accessSpec), std::move(bucketInfo), std::move(settings),
    std::move(macroValues), std::move(headers)
  };
  auto str = TableSpec::serialize(spec);

  // copy elision
  auto ptr = TableSpec::deserialize(str);
  TableSpec spec2 = *ptr;
  EXPECT_EQ(spec2.name, "abc");
  EXPECT_EQ(spec2.max_mb, 1);
  EXPECT_EQ(spec2.max_seconds, 2);
  EXPECT_EQ(spec2.schema, "ROW<id:bigint>");
  EXPECT_EQ(spec2.source, DataSource::S3);
  EXPECT_EQ(spec2.loader, "Roll");
  EXPECT_EQ(spec2.location, "s3://nebula");
  EXPECT_EQ(spec2.backup, "s3://backup");
  EXPECT_EQ(spec2.format, DataFormat::JSON);
  EXPECT_EQ(spec2.csv.hasHeader, true);
  EXPECT_EQ(spec2.csv.delimiter, "sep");
  EXPECT_EQ(spec2.json.rowsField, "rows");
  EXPECT_EQ(spec2.json.columnsMap.size(), 1);
  EXPECT_EQ(spec2.json.columnsMap.at("col1"), "field1");
  EXPECT_EQ(spec2.thrift.protocol, "binary");
  EXPECT_EQ(spec2.thrift.columnsMap.size(), 1);
  EXPECT_EQ(spec2.thrift.columnsMap.at("col1"), 1);
  EXPECT_EQ(spec2.kafkaSerde.retention, 1);
  EXPECT_EQ(spec2.kafkaSerde.size, 2);
  EXPECT_EQ(spec2.kafkaSerde.topic, "topic");
  EXPECT_EQ(spec2.rocksetSerde.apiKey, "key");
  EXPECT_EQ(spec2.rocksetSerde.interval, 5);
  EXPECT_EQ(spec2.columnProps.size(), 1);
  EXPECT_EQ(spec2.columnProps.at("c1").withBloomFilter, false);
  EXPECT_EQ(spec2.timeSpec.type, TimeType::STATIC);
  EXPECT_EQ(spec2.timeSpec.unixTimeValue, 123);
  EXPECT_EQ(spec2.timeSpec.column, "ct");
  EXPECT_EQ(spec2.accessSpec.size(), 1);
  EXPECT_EQ(spec2.accessSpec.at(0).type, AccessType::READ);
  EXPECT_EQ(spec2.accessSpec.at(0).groups.size(), 1);
  EXPECT_EQ(spec2.accessSpec.at(0).groups.at(0), "nebula-users");
  EXPECT_EQ(spec2.accessSpec.at(0).action, ActionType::MASK);
  EXPECT_EQ(spec2.settings.size(), 2);
  EXPECT_EQ(spec2.settings.at("key1"), "value1");
  EXPECT_EQ(spec2.settings.at("key2"), "value2");
}
} // namespace test
} // namespace meta
} // namespace nebula
