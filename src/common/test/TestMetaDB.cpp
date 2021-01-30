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
#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include "common/Finally.h"

/**
 * Test all stats algorithms provided by folly/stats package
 */
namespace nebula {
namespace common {
namespace test {

TEST(CommonTest, TestLevelDB) {
  leveldb::DB* db = nullptr;

  leveldb::Options options;
  options.create_if_missing = true;
  options.block_cache = leveldb::NewLRUCache(100 * 1048576);
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  EXPECT_TRUE(status.ok());

  // do a clean up
  nebula::common::Finally clean{ [&options, &db]() {
    if (db) {
      delete db;
    }
    if (options.block_cache) {
      delete options.block_cache;
    }
  } };

  // add some key-value
  leveldb::WriteOptions wopts;
  leveldb::Slice key{ "key" };
  leveldb::Slice value{ "value" };
  status = db->Put(wopts, key, value);
  status = db->Put(wopts, key, value);
  EXPECT_TRUE(status.ok());

  // read it back
  leveldb::ReadOptions ropts;
  std::string read;
  status = db->Get(ropts, key, &read);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(read, value.ToString());

  // delete the key
  status = db->Delete(wopts, key);
  EXPECT_TRUE(status.ok());
  status = db->Get(ropts, key, &read);
  EXPECT_TRUE(status.IsNotFound());

  // write it again
  status = db->Put(wopts, key, value);
  EXPECT_TRUE(status.ok());

  // close the DB and reopen it
  delete db;
  status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  EXPECT_TRUE(status.ok());

  // read the value written previously
  status = db->Get(ropts, key, &read);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(read, value.ToString());

  // test batch write
  leveldb::WriteBatch batch;
  batch.Delete(key);
  batch.Put(key, "abc");
  batch.Delete(key);
  batch.Delete(key);
  batch.Put(key, "xyz");
  status = db->Write(wopts, &batch);
  EXPECT_TRUE(status.ok());
  status = db->Get(ropts, key, &read);
  EXPECT_TRUE(status.ok());
  // batch executed in sequence, last op wins
  EXPECT_EQ(read, "xyz");

  // concurrency
  // ref: https://github.com/google/leveldb/blob/master/doc/index.md
  // A database may only be opened by one process at a time.
  // The leveldb implementation acquires a lock from the operating system to prevent misuse.
  // Within a single process, the same leveldb::DB object may be safely shared by multiple concurrent threads.
  // I.e., different threads may write into or fetch iterators or call Get on the same database
  // without any external synchronization (the leveldb implementation will automatically do the required synchronization).
  // However other objects (like Iterator and WriteBatch) may require external synchronization.
  // If two threads share such an object, they must protect access to it using their own locking protocol.
  // More details are available in the public header files.
  // [print all keys]
  auto it = std::unique_ptr<leveldb::Iterator>(db->NewIterator(ropts));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    LOG(INFO) << "Key: " << it->key().ToString() << ": " << it->value().ToString();
  }

  // take a snapshot and write some new keys
  // reading on the snapshot will not include the new added keys
  // set the snapshot to read option so that reading with this option will be applied on the snapshot
  ropts.snapshot = db->GetSnapshot();
  status = db->Put(wopts, key, "123");
  EXPECT_TRUE(status.ok());
  status = db->Get(ropts, key, &read);
  EXPECT_TRUE(status.ok());
  // the snapshot has value of "xyz" rather than the new overwrite "123";
  EXPECT_EQ(read, "xyz");
  // release the snapshot and read the default state instead
  db->ReleaseSnapshot(ropts.snapshot);
  ropts.snapshot = nullptr;
  status = db->Get(ropts, key, &read);
  EXPECT_TRUE(status.ok());
  // the snapshot has value of "xyz" rather than the new overwrite "123";
  EXPECT_EQ(read, "123");

  // Notes:
  // slice is basically std::string_view, it depends on external bytes to be live through its life cycle.
  // compression is by default auto applied (disabled on uncompressible block)
  // cache: options.block_cache is non-NULL, it is used to cache frequently used uncompressed block contents
  // key layout: keys are sorted, so if you're access some keys more often than others,
  // thinking about how to augment those keys so that they can be next to each other in the same block.

  // filters, use bloom fiter policy will ask leveldb to build bloom filter index in memory to reduce disk scan.
  // be careful if comparactor doesn't respect the whole key (e.g removing trailing space before comparing)
  // a custom filter is needed if above situation happens
}

} // namespace test
} // namespace common
} // namespace nebula