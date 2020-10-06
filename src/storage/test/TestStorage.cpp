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
#include <fstream>
#include <glog/logging.h>
#include <gtest/gtest.h>

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

TEST(StorageTest, TestLocalCopy) {
  LOG(INFO) << "Run storage test here";
  auto fs = nebula::storage::makeFS("local");
  EXPECT_TRUE(fs->sync("configs", "/tmp/testconfigs"));
}

TEST(StorageTest, DISABLED_TestS3Api) {
  auto fs = nebula::storage::makeFS("s3", "<bucket>");
  auto keys = fs->list("nebula/pin_messages/");
  for (auto& key : keys) {
    LOG(INFO) << "key: " << key.name;
  }

  // display content of one key
  LOG(INFO) << "Total keys: " << keys.size();
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

TEST(StorageTest, DISABLED_TestS3Copy) {
  auto fs = nebula::storage::makeFS("s3", "<bucket>");
  auto lfs = nebula::storage::makeFS("local");
  auto local = lfs->temp();
  fs->copy("nebula/pin_pins/cd=2019-08-31/000117", local);
  auto fi = lfs->info(local);

  LOG(INFO) << "file info: " << fi.signature();
}

TEST(StorageTest, DISABLED_TestS3Sync) {
  auto fs = nebula::storage::makeFS("s3", "<bucket>");
  auto lfs = nebula::storage::makeFS("local");
  auto local = lfs->temp(true);
  LOG(INFO) << "sync all files to: " << local;
  fs->sync("nebula/s3_cost", local);

  for (auto& x : lfs->list(local)) {
    LOG(INFO) << "file: " << x.name;
  }
}

TEST(StorageTest, DISABLED_TestRoundTrip) {
  auto fs = nebula::storage::makeFS("s3", "<bucket>");
  auto lfs = nebula::storage::makeFS("local");
  auto remote = "nebula/trt";
  // write a file to it
  auto local1 = lfs->temp(true);
  {
    std::ofstream f1{ fmt::format("{0}/1", local1) };
    f1 << "abc";
    f1.close();
  }

  // sync it to s3://pinlogs/nebula/mdb
  EXPECT_TRUE(fs->sync(local1, remote));

  // list s3
  {
    auto files = fs->list(remote);
    EXPECT_EQ(1, files.size());
    EXPECT_EQ(fmt::format("{0}/1", remote), files.at(0).name);
  }
  // sync the s3 folder to local another folder
  auto local2 = lfs->temp(true);
  EXPECT_TRUE(fs->sync(remote, local2));

  // list the new folder
  {
    auto files = lfs->list(local2);
    EXPECT_EQ(1, files.size());
    EXPECT_EQ("1", files.at(0).name);
  }

  LOG(INFO) << "Sync works for all: local1=" << local1
            << ", remote=" << remote
            << ", local2=" << local2;
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

  {
    // try to support normal file
    auto uriInfo = nebula::storage::parse("/etc/nebula/configs/cluster.yml");
    EXPECT_EQ(uriInfo.schema, "");
    EXPECT_EQ(uriInfo.host, "");
    EXPECT_EQ(uriInfo.path, "etc/nebula/configs/cluster.yml");
  }
}

} // namespace test
} // namespace storage
} // namespace nebula