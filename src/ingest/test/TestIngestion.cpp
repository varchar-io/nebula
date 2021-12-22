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

#include <common/Evidence.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <pg_query.h>
#include <storage/NFS.h>

#include "ingest/IngestSpec.h"
#include "ingest/SpecRepo.h"
#include "meta/ClusterInfo.h"
#include "meta/MetaDb.h"
#include "meta/TableSpec.h"

namespace nebula {
namespace ingest {
namespace test {

using nebula::ingest::SpecRepo;
using namespace nebula::meta;

TEST(IngestTest, TestIngestSpec) {
  CsvProps csvProps;
  JsonProps jsonProps;
  ThriftProps thriftProps;
  KafkaSerde kafkaSerde;
  RocksetSerde rocksetSerde;
  TimeSpec timeSpec;
  AccessSpec accSpec;
  ColumnProps colProps;
  BucketInfo bucketInfo = BucketInfo::empty();
  std::unordered_map<std::string, std::string> settings;
  auto table = std::make_shared<TableSpec>(
    "test", 1000, 10, "s3", DataSource::S3,
    "swap", "s3://test", "s3://bak",
    DataFormat::CSV, std::move(csvProps), std::move(jsonProps), std::move(thriftProps),
    std::move(kafkaSerde), std::move(rocksetSerde),
    std::move(colProps), std::move(timeSpec),
    std::move(accSpec), std::move(bucketInfo), std::move(settings));
  nebula::ingest::IngestSpec spec(table, "1.0", "nebula/v1.x", "nebula", 10, SpecState::NEW, 0);
  LOG(INFO) << "SPEC: " << spec.toString();
  EXPECT_EQ(spec.id(), "test@nebula/v1.x@10");
  EXPECT_EQ(spec.size(), 10);
  EXPECT_EQ(spec.path(), "nebula/v1.x");
  EXPECT_EQ(spec.domain(), "nebula");
  EXPECT_EQ(spec.table()->name, "test");
  EXPECT_EQ(spec.version(), "1.0");
}

TEST(IngestTest, TestSpecGeneration) {
#ifndef __APPLE__
  nebula::ingest::SpecRepo sr;

  // load cluster info from sample config
  auto& ci = nebula::meta::ClusterInfo::singleton();
  ci.load("configs/cluster.yml", [](const nebula::meta::MetaConf&) {
    return std::make_unique<nebula::meta::VoidDb>();
  });

  // refresh spec repo with the ci object
  sr.refresh(ci);

  // check sr states with number of specs generated and their status
  const auto& specs = sr.specs();
  for (auto itr = specs.cbegin(), end = specs.cend(); itr != end; ++itr) {
    LOG(INFO) << fmt::format("ID={0}, Spec={1}", itr->first, itr->second->toString());
  }
#endif
}

TEST(IngestTest, TestTransformerAddColumn) {
  // schema: "ROW<id:int, event:string, items:list<string>, flag:bool, value:tinyint>"
  auto transformer = pg_query_parse("SELECT id, event, items, flag, value, to_unixtime(now) from nebula.test");
  LOG(INFO) << transformer.parse_tree;
  pg_query_free_parse_result(transformer);
}

TEST(IngestTest, TestTimePatternSpecGeneration) {
  nebula::ingest::SpecRepo sr;

  // load cluster info from sample config
  auto& ci = nebula::meta::ClusterInfo::singleton();
  ci.load("configs/test.yml", [](const nebula::meta::MetaConf&) {
    return std::make_unique<nebula::meta::VoidDb>();
  });

  sr.refresh(ci);

  // test macro parser
  const auto& specs = ci.tables();
  for (auto spec : specs) {
    const auto type = spec->timeSpec.type;
    const auto name = spec->name;
    if (type == meta::TimeType::MACRO && name == "nebula.hourly") {
      const auto sourceInfo = nebula::storage::parse(spec->location);
      std::string macroStr = "hour";

      int pos = sourceInfo.path.find(macroStr);
      std::string pathTemplate = sourceInfo.path;
      pathTemplate.replace(pos, macroStr.size(), "2020-01-01");
      EXPECT_EQ(pathTemplate.substr(pos, strlen("2020-01-01")), "2020-01-01");

      // scan four hour ago
      auto ingestSpec = std::vector<std::shared_ptr<IngestSpec>>();
      // always include current hour
      SpecRepo::genPatternSpec(meta::PatternMacro::HOURLY,
                               sourceInfo.path,
                               common::Evidence::HOUR_SECONDS * 4,
                               "1.0", spec, ingestSpec);
    } else if (type == meta::TimeType::MACRO && name == "nebula.daily") {
      const auto sourceInfo = nebula::storage::parse(spec->location);
      std::string macroStr = "date";
      // scan an hour ago
      auto ingestSpec = std::vector<std::shared_ptr<IngestSpec>>();
      // TODO (chenqin): mock s3 filesystem list
      // expect cover last day
      SpecRepo::genPatternSpec(meta::PatternMacro::DAILY, sourceInfo.path, 3600,
                               "1.0", spec, ingestSpec);
    }
  }
}
} // namespace test
} // namespace ingest
} // namespace nebula
