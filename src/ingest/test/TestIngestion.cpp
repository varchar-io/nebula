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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "ingest/IngestSpec.h"
#include "ingest/SpecRepo.h"
#include "meta/ClusterInfo.h"
#include "meta/TableSpec.h"

namespace nebula {
namespace ingest {
namespace test {

TEST(IngestTest, TestIngestSpec) {
  nebula::meta::TimeSpec ts;
  nebula::meta::ColumnProps cp;
  nebula::meta::Serde sd;
  auto table = std::make_shared<nebula::meta::TableSpec>(
    "test", 1000, 10, "s3", nebula::meta::DataSource::S3,
    "swap", "s3://test", "s3://bak", "csv", sd, cp, ts);
  nebula::ingest::IngestSpec spec(table, "1.0", "nebula/v1.x", "nebula", 10, SpecState::NEW, 0);
  LOG(INFO) << "SPEC: " << spec.toString();
}

TEST(IngestTest, TestSpecGeneration) {
#ifndef __APPLE__
  nebula::ingest::SpecRepo sr;

  // load cluster info from sample config
  auto& ci = nebula::meta::ClusterInfo::singleton();
  ci.load("configs/cluster.yml");

  // refresh spec repo with the ci object
  sr.refresh(ci);

  // check sr states with number of specs generated and their status
  const auto& specs = sr.specs();
  for (auto itr = specs.cbegin(), end = specs.cend(); itr != end; ++itr) {
    LOG(INFO) << fmt::format("ID={0}, Spec={1}", itr->first, itr->second->toString());
  }
#endif
}
} // namespace test
} // namespace ingest
} // namespace nebula