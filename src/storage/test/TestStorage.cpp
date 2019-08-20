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
#include "storage/NFS.h"
#include "storage/aws/S3.h"
#include "storage/local/File.h"

namespace nebula {
namespace storage {
namespace test {

TEST(StorageTest, TestLocalFiles) {
  LOG(INFO) << "Run storage test here";
  auto fs = nebula::storage::makeFS("local");
  auto files = fs->list(".");
  for (auto& f : files) {
    LOG(INFO) << "File: " << f.name;
  }

  EXPECT_TRUE(files.size() > 0);
}

TEST(StorageTest, TestLoadingCsv) {
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

TEST(StorageTest, DISABLED_TestS3Api) {
  auto fs = nebula::storage::makeFS("s3", "pinlogs");
  auto keys = fs->list("nebula/pin_signatures/");
  for (auto& key : keys) {
    LOG(INFO) << "key: " << key.name;
  }

  // display content of one key
  if (keys.size() > 0) {
    auto objs = fs->list(keys.front().name);
    int count = 0;
    for (auto& key : objs) {
      LOG(INFO) << "key: " << key.name;
      if (count++ > 10) {
        break;
      }
    }
  }
}

TEST(StorageTest, TestUriParse) {
  {
    auto uriInfo = nebula::storage::parse("http://who/is/nebula/");
    EXPECT_EQ(uriInfo.schema, "http");
    EXPECT_EQ(uriInfo.host, "who");
    EXPECT_EQ(uriInfo.path, "is/nebula");
  }
  {
    auto uriInfo = nebula::storage::parse("s3://pitfall/prefix/nebula/a.txt");
    EXPECT_EQ(uriInfo.schema, "s3");
    EXPECT_EQ(uriInfo.host, "pitfall");
    EXPECT_EQ(uriInfo.path, "prefix/nebula/a.txt");
  }
  {
    auto uriInfo = nebula::storage::parse("file:///var/log/log.txt");
    EXPECT_EQ(uriInfo.schema, "file");
    EXPECT_EQ(uriInfo.host, "");
    EXPECT_EQ(uriInfo.path, "var/log/log.txt");
  }
  {
    // testing macro replacement using <date> not supported by uri parser
    // use $date as macro name will be supported
    auto uriInfo = nebula::storage::parse("s3://x/y/cd=$date$");
    EXPECT_EQ(uriInfo.schema, "s3");
    EXPECT_EQ(uriInfo.host, "x");
    EXPECT_EQ(uriInfo.path, "y/cd=$date$");
  }

  {
    // try to support libfmt formatting placeholder
    auto uriInfo = nebula::storage::parse("s3://x/y/cd=%7Bdate%7D");
    EXPECT_EQ(uriInfo.schema, "s3");
    EXPECT_EQ(uriInfo.host, "x");
    EXPECT_EQ(uriInfo.path, "y/cd={date}");
  }
}

} // namespace test
} // namespace storage
} // namespace nebula