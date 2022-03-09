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

#include "meta/ClusterInfo.h"
#include "meta/NBlock.h"
#include "meta/Pod.h"
#include "meta/TableSpec.h"
#include "meta/TestTable.h"
#include "type/Serde.h"

namespace nebula {
namespace meta {
namespace test {

TEST(MetaTest, TestTestTable) {
  nebula::meta::TestTable test;
  LOG(INFO) << "Table provides table logic data and physical data";
  LOG(INFO) << "Test table name: " << test.name();
  LOG(INFO) << "Test table schema: " << test.schema();
}

TEST(MetaTest, TestNBlock) {
  BlockState state;
  NBlock<int> b1(BlockSignature{ "mock", 0, 5, 10 }, NNode::inproc(), state);

  // check in range
  ASSERT_TRUE(b1.inRange(5));
  ASSERT_TRUE(b1.inRange(10));
  ASSERT_TRUE(b1.inRange(7));
  ASSERT_FALSE(b1.inRange(11));
  ASSERT_FALSE(b1.inRange(1));

  // check overlap
  ASSERT_TRUE(b1.overlap({ 10, 11 }));
  ASSERT_TRUE(b1.overlap({ 5, 11 }));
  ASSERT_TRUE(b1.overlap({ 2, 11 }));
  ASSERT_TRUE(b1.overlap({ 2, 5 }));
  ASSERT_FALSE(b1.overlap({ 2, 4 }));
  ASSERT_FALSE(b1.overlap({ 12, 18 }));
}

TEST(MetaTest, TestNNode) {
  NNode n1{ NRole::NODE, "1.0.0.1", 90 };
  NNode n2{ n1 };
  ASSERT_TRUE(n1.equals(n2));
  LOG(INFO) << "N2=" << n2.toString();
}

TEST(MetaTest, TestDataSource) {
  using ds = nebula::meta::DataSource;
  using dsu = nebula::meta::DataSourceUtils;

  // test isFileSystem
  {
    EXPECT_TRUE(dsu::isFileSystem(ds::S3));
    EXPECT_TRUE(dsu::isFileSystem(ds::GS));
    EXPECT_TRUE(dsu::isFileSystem(ds::LOCAL));
    EXPECT_FALSE(dsu::isFileSystem(ds::HTTP));
    EXPECT_FALSE(dsu::isFileSystem(ds::KAFKA));
    EXPECT_FALSE(dsu::isFileSystem(ds::NEBULA));
    EXPECT_FALSE(dsu::isFileSystem(ds::GSHEET));
  }

  // test get protocol
  {
    EXPECT_EQ("s3", dsu::getProtocol(ds::S3));
    EXPECT_EQ("gs", dsu::getProtocol(ds::GS));
    EXPECT_EQ("http", dsu::getProtocol(ds::HTTP));
    EXPECT_EQ("local", dsu::getProtocol(ds::LOCAL));
    EXPECT_EQ("", dsu::getProtocol(ds::KAFKA));
    EXPECT_EQ("", dsu::getProtocol(ds::NEBULA));
    EXPECT_EQ("", dsu::getProtocol(ds::GSHEET));
  }

  // test get data source from name
  {
    EXPECT_EQ(ds::S3, dsu::from("S3"));
    EXPECT_EQ(ds::GS, dsu::from("gs"));
    EXPECT_EQ(ds::HTTP, dsu::from("http"));
    EXPECT_EQ(ds::HTTP, dsu::from("HTTPs"));
    EXPECT_EQ(ds::LOCAL, dsu::from("local"));
    EXPECT_EQ(ds::KAFKA, dsu::from("KAFKA"));
    EXPECT_EQ(ds::NEBULA, dsu::from(""));
    EXPECT_EQ(ds::NEBULA, dsu::from("xyz"));
  }
}

TEST(MetaTest, TestClusterConfigLoad) {
  auto yamlFile = "configs/test.yml";
  auto& clusterInfo = nebula::meta::ClusterInfo::singleton();
  clusterInfo.load(yamlFile, [](const nebula::meta::MetaConf&) {
    return std::unique_ptr<nebula::meta::MetaDb>(new nebula::meta::VoidDb());
  });

  // VoidDB now return false in its interfaces
  EXPECT_FALSE(clusterInfo.db().write("key", "value"));

  // verify data against config file
  const auto& nodes = clusterInfo.nodes();
  EXPECT_EQ(nodes.size(), 1);
  for (auto itr = nodes.cbegin(); itr != nodes.cend(); ++itr) {
    LOG(INFO) << "NODE: " << itr->toString();
  }

  const auto& tables = clusterInfo.tables();
  nebula::meta::TableSpecPtr test = nullptr;
  nebula::meta::TableSpecPtr ephemeral = nullptr;
  EXPECT_EQ(tables.size(), 3);
  for (auto itr = tables.cbegin(); itr != tables.cend(); ++itr) {
    LOG(INFO) << "TABLE: " << (*itr)->toString();
    if ((*itr)->name == "nebula.daily") {
      ephemeral = (*itr);
    }
    if ((*itr)->name == "nebula.test") {
      test = (*itr);
    }
  }

  // on-demand ephemeral pacakge are loaded through API
  auto schema = nebula::type::TypeSerializer::from(ephemeral->schema);
  EXPECT_EQ(schema->size(), 18);
  EXPECT_EQ(ephemeral->loader, "Api");
  EXPECT_EQ(ephemeral->bucketInfo.count, 10000);
  EXPECT_EQ(ephemeral->bucketInfo.bucketColumn, "partner_id");
  // the bucket column can be found from the schema
  auto type = schema->find(ephemeral->bucketInfo.bucketColumn);
  EXPECT_TRUE(type != nullptr);
  EXPECT_TRUE(nebula::type::TypeBase::isScalar(type->k()));

  // test the table level settings can be read as expected
  EXPECT_EQ(test->settings.size(), 2);
  EXPECT_EQ(test->settings.at("key1"), "value1");
  EXPECT_EQ(test->settings.at("key2"), "value2");
  LOG(INFO) << "key1=" << test->settings.at("key1");

  // test the test table has headers loaded correctly
  EXPECT_EQ(test->headers.size(), 2);
  EXPECT_EQ(test->headers.at(0), "header1: 1");
  EXPECT_EQ(test->headers.at(1), "header2: 2");
}

TEST(MetaTest, TestRuntimeTableDefinition) {
  auto yamlFile = "configs/test.yml";
  auto& clusterInfo = nebula::meta::ClusterInfo::singleton();

  // invalid
  auto error = clusterInfo.addTable("k.test", "");
  EXPECT_TRUE(error.size() > 0);
  LOG(ERROR) << "Error adding table: " << error;

  // as long as there is one element, it will be treated as valid
  error = clusterInfo.addTable("k.test", "data: xyz");
  EXPECT_TRUE(error.size() == 0);

  auto yamlTableDef = R"(
    retention:
      max-mb: 20000
      max-hr: 24
    schema: "ROW<name:string, value:int>"
    data: kafka
    loader: Streaming
    source: broker:9092
    backup: s3://nebula/n116/
    format: json
    kafka:
      topic: test2
    columns:
      name:
        dict: true
    time:
      # kafka will inject a time column when specified provided
      type: provided
    settings:
      batch: 100
      kafka.timeout: 100
    )";
  error = clusterInfo.addTable("k.test", yamlTableDef);
  EXPECT_EQ(error.size(), 0);

  // add some runtime table into it and make sure it loads
  clusterInfo.load(yamlFile, [](const nebula::meta::MetaConf&) {
    return std::unique_ptr<nebula::meta::MetaDb>(new nebula::meta::VoidDb());
  });

  // make sure we have table that we just added in the runtime
  // in addition to the ones loaded from static config
  const auto& tables = clusterInfo.tables();
  nebula::meta::TableSpecPtr runtime = nullptr;
  EXPECT_EQ(tables.size(), 4);
  for (auto itr = tables.cbegin(); itr != tables.cend(); ++itr) {
    LOG(INFO) << "TABLE: " << (*itr)->toString();
    if ((*itr)->name == "k.test") {
      runtime = (*itr);
    }
  }
  EXPECT_EQ(runtime->name, "k.test");
  EXPECT_EQ(runtime->schema, "ROW<name:string, value:int>");
  EXPECT_EQ(runtime->loader, "Streaming");
  EXPECT_EQ(runtime->source, DataSource::KAFKA);
  EXPECT_EQ(runtime->format, DataFormat::JSON);
  EXPECT_EQ(runtime->settings.size(), 2);
  EXPECT_EQ(runtime->settings.at("batch"), "100");
  EXPECT_EQ(runtime->settings.at("kafka.timeout"), "100");
}

TEST(MetaTest, TestAccessRules) {
  auto schema = nebula::type::TypeSerializer::from(
    "ROW<_time_: bigint, id:int, event:string, email:string, fund:bigint>");

  // column level access control - mask email when user is not in the group of pii_sg
  ColumnProps columnProps;
  columnProps["email"] = Column{
    false,
    false,
    false,
    "*@*",
    "",
    { AccessRule{ AccessType::READ, { "pii_sg" }, ActionType::MASK } }
  };
  columnProps["fund"] = Column{
    false,
    false,
    false,
    "0",
    "",
    { AccessRule{ AccessType::READ, { "pii_sg" }, ActionType::DENY } }
  };

  // no table level access control, pass empty {} access rules
  nebula::meta::Table test{
    "pii_table",
    schema,
    columnProps,
    {}
  };

  // assume user has two normal groups
  nebula::common::unordered_set<std::string> groups;
  groups.emplace("eng");
  groups.emplace("ads");

  // run check access function to ensure it works as expected
#define TEST_COL_READ_ACCESS(COL, EXPECT) \
  EXPECT_EQ(test.checkAccess(AccessType::READ, groups, COL), ActionType::EXPECT);

  // 1. table access - pass
  EXPECT_EQ(test.checkAccess(AccessType::READ, groups), ActionType::PASS);

  // 2. column id access - pass
  TEST_COL_READ_ACCESS("id", PASS)

  // 3. column event access - pass
  TEST_COL_READ_ACCESS("event", PASS)

  // 4. column email access - mask
  TEST_COL_READ_ACCESS("email", MASK)

  // 5. column fund access - deny
  TEST_COL_READ_ACCESS("fund", DENY)

  // now, let's put this user into pii_sg group and run the test again
  groups.emplace("pii_sg");
  TEST_COL_READ_ACCESS("id", PASS)
  TEST_COL_READ_ACCESS("event", PASS)
  TEST_COL_READ_ACCESS("email", PASS)
  TEST_COL_READ_ACCESS("fund", PASS)

#undef TEST_COL_READ_ACCESS
}

TEST(MetaTest, TestPod) {
  nebula::meta::Pod::KeyList keys;
  keys.reserve(3);

  keys.emplace_back(
    new PartitionKey<std::string>("country",
                                  { "US", "CA", "FR", "BR", "GB" },
                                  2));
  keys.emplace_back(
    new PartitionKey<std::string>("gender",
                                  { "Male", "Female", "Unknown" }));
  keys.emplace_back(
    new PartitionKey<int8_t>("age",
                             { 1, 2, 3, 4, 5, 6, 7, 8, 9 },
                             2));

  nebula::meta::Pod pod{ std::move(keys) };
  auto capacity = pod.capacity();
  auto numKeys = pod.numKeys();
  auto bessWidth = pod.bessBits();
  LOG(INFO) << "the pod with " << numKeys
            << " dimensions has max blocks: " << capacity
            << ", each bess value length=" << bessWidth;

  // list offset for each key: 1, 3, 9
  for (size_t i = 0; i < numKeys; ++i) {
    LOG(INFO) << "key: " << i << ", offset:" << (int)pod.offset(i);
  }

  // for any combination, we can locate which pod this value belongs to
  {
    // "US"->0, "Unknown"->2, 7->3, so pid = 0*1 + 2*3 + 3 * 9 = 31
    BessType bess;
    EXPECT_EQ(pod.pod({ "US", "Unknown", "7" }, bess), 33);
    LOG(INFO) << "pid=33, bess=" << bess;
    // reverse operation
    int v;
    EXPECT_FALSE(pod.value<int>("xy", { 0 }, 0, v));

    // get space of each dimension from given pod id
    auto spaces = pod.locate(33);
    // reverse country, gender, age
    std::string_view country;
    EXPECT_TRUE(pod.value("country", spaces, 0, country));
    EXPECT_EQ(country, "US");
    std::string_view gender;
    EXPECT_TRUE(pod.value("gender", spaces, 0, gender));
    EXPECT_EQ(gender, "Unknown");
    int8_t age;
    EXPECT_TRUE(pod.value("age", spaces, 0, age));
    EXPECT_EQ(age, 7);
    LOG(INFO) << "country=" << country << ", gender=" << gender << ", age=" << age;
  }

  {
    BessType bess;
    // "CA"->0, "Male"->0, 4->1, so pid = 0*1 + 0*3 + 1 * 9 = 9
    EXPECT_EQ(pod.pod({ "CA", "Male", "4" }, bess), 9);
    LOG(INFO) << "pid=9, bess=" << bess;
    // "GB"->2, "Female"->1, 9->4, so pid = 2*1 + 1*3 + 4 * 9 = 9
    EXPECT_EQ(pod.pod({ "GB", "Female", "9" }, bess), 41);
    LOG(INFO) << "pid=41, bess=" << bess;
  }

  // test pod ID range for given predicates
  {
    // [0, 0], [1, 1], [1, 3] =>
    // low = 0*1 + 1*3 + 1*9 = 12
    // high = 0*1 + 1*3 + 3*9 = 30
    Span expected{ 12, 30 };
    auto range = pod.span({ { "US" }, { "Female" }, { "3", "8" } });
    EXPECT_EQ(range, expected);
  }
  {
    // [0, 2], [2, 2], [2, 2] =>
    // low = 0*1 + 2*3 + 2*9 = 24
    // high = 2*1 + 2*3 + 2*9 = 26
    Span expected{ 24, 26 };
    auto range = pod.span({ {}, { "Unknown" }, { "5" } });
    EXPECT_EQ(range, expected);
  }

  {
    // TODO(cao): current mapping will fast reduce pods range if high cardinality spaces in low dimension
    // So we should sort all dimensions by number of spaces in decending order
    // [0, 2], [0, 2], [2, 4] =>
    // low = 0*1 + 0*3 + 2*9 = 18
    // high = 2*1 + 2*3 + 4*9 = 44
    Span expected{ 18, 44 };
    auto range = pod.span({ {}, {}, { "5", "9" } });
    EXPECT_EQ(range, expected);
  }

  // test reverse pid to dimensions
  {
    std::vector<size_t> expected{ 2, 1, 4 };
    EXPECT_EQ(pod.locate(41), expected);
  }
  {
    std::vector<size_t> expected{ 1, 0, 4 };
    EXPECT_EQ(pod.locate(37), expected);
  }
  {
    std::vector<size_t> expected{ 2, 1, 3 };
    EXPECT_EQ(pod.locate(32), expected);
  }
}

} // namespace test
} // namespace meta
} // namespace nebula