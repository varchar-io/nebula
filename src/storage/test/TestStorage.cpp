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
#include "storage/CsvReader.h"
#include "storage/local/File.h"

namespace nebula {
namespace storage {
namespace test {

TEST(StorageTest, TestLocalFiles) {
  LOG(INFO) << "Run storage test here";
  auto files = nebula::storage::local::File::list(".");
  for (auto& f : files) {
    LOG(INFO) << "File: " << f;
  }

  EXPECT_TRUE(files.size() > 0);
}

TEST(LocalCsvTest, TestLoadingCsv) {
  auto file = "/Users/shawncao/trends.draft.csv";
  CsvReader reader(file);
  int count = 0;
  while (reader.hasNext()) {
    auto& row = reader.next();
    auto method = row.readString("methodology");
    if (method == "test_data_limit_100000") {
      count++;
      // LOG(INFO) << fmt::format("{0:10} | {1:10} | {2:10}",
      //                          row.readString("query"),
      //                          row.readString("dt"),
      //                          row.readInt("count"));
    }
  }

  LOG(INFO) << " Total rows loaded: " << count;
}

TEST(AwsCsvTest, TestLoadingCsv) {
  // nebula::storage::aws::S3 s3("pinterest-seattle");
  // auto keys = s3.list("db_snapshot/comments/dt=2019-06-05/");
  // for (auto& key : keys) {
  //   LOG(INFO) << "key: " << key;
  // }
  // CsvReader reader(file);
  // int count = 0;
  // while (reader.hasNext()) {
  //   auto& row = reader.next();
  //   auto method = row.readString("methodology");
  //   if (method == "test_data_limit_100000") {
  //     count++;
  //     // LOG(INFO) << fmt::format("{0:10} | {1:10} | {2:10}",
  //     //                          row.readString("query"),
  //     //                          row.readString("dt"),
  //     //                          row.readInt("count"));
  //   }
  // }

  // LOG(INFO) << " Total rows loaded: " << count;
}
} // namespace test
} // namespace storage
} // namespace nebula