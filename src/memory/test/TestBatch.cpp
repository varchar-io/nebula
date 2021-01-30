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
#include <valarray>

#include "common/Memory.h"
#include "memory/Batch.h"
#include "memory/DataNode.h"
#include "memory/FlatRow.h"
#include "meta/TestTable.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "surface/StaticData.h"
#include "type/Serde.h"

namespace nebula {
namespace memory {
namespace test {

using nebula::common::Evidence;
using nebula::surface::IndexType;
using nebula::surface::MockRowData;
using nebula::type::ROOT;
using nebula::type::TypeSerializer;

TEST(BatchTest, TestBatchSize) {
  // empty batch should not allocate much memory
  nebula::meta::TestTable test;

  auto print = [](Batch& b) {
    LOG(INFO) << "batch size= " << sizeof(b);
    LOG(INFO) << "batch state= " << b.state();
    LOG(INFO) << "batch memory= " << b.getMemory();
  };

  Batch batch1(test, 1000);
  LOG(INFO) << "batch1 size= " << sizeof(batch1);
  print(batch1);

  Batch batch2(test, 1000000);
  LOG(INFO) << "batch2 size= " << sizeof(batch2);
  print(batch2);
}

TEST(BatchTest, TestBatch) {
  nebula::meta::TestTable test;
  // add 10 rows
  auto rows = 10;

  Batch batch1(test, rows);
  Batch batch2(test, rows);

  // use the specified seed so taht the data can repeat
  auto seed = Evidence::unix_timestamp();

  // do write
  {
    MockRowData row(seed);
    for (auto i = 0; i < rows; ++i) {
      batch1.add(row);
    }

    // verify batch data
    EXPECT_EQ(batch1.getRows(), rows);

    // print out the batch state
    batch1.seal();
    LOG(INFO) << "Batch-Memory: " << batch1.getMemory();
    LOG(INFO) << "Batch: " << batch1.state();
  }

  {
    MockRowData row(seed);
    for (auto i = 0; i < rows; ++i) {
      batch2.add(row);
    }

    // verify batch data
    EXPECT_EQ(batch2.getRows(), rows);

    // print out the batch state
    batch2.seal();
    LOG(INFO) << "Batch: " << batch2.state();
  }

  EXPECT_EQ(batch1.state(), batch2.state());
}

TEST(BatchTest, TestBatchRead) {
  nebula::meta::TestTable test;
  auto count = 10000;

  // need some stable data set to write out and can be verified
  Batch batch(test, count);

  // use the specified seed so taht the data can repeat
  std::vector<nebula::surface::StaticRow> rows;
  auto seed = nebula::common::Evidence::ticks();
  MockRowData row{ seed };
  // fill rows
  for (auto i = 0; i < count; ++i) {
    rows.push_back({ row.readLong("_time_"),
                     row.readInt("id"),
                     row.readString("event"),
                     i % 3 != 0 ? nullptr : row.readList("items"),
                     // row.isNull("items") ? nullptr : row.readList("items"),
                     row.readBool("flag"),
                     row.readByte("value"),
                     row.readInt128("i128"),
                     row.readDouble("weight") });
  }

  // print single row as string.

  auto line = [](const nebula::surface::RowData& r) {
    // std::string s;
    // if (!r.isNull("items")) {
    //   const auto list = r.readList("items");
    //   for (auto k = 0; k < list->getItems(); ++k) {
    //     s += fmt::format("{0},", list->readString(k));
    //   }
    // }

    return fmt::format("({0}, {1}, {2})",
                       r.readInt("id"), r.readString("event"), r.readBool("flag"));
  };
  auto line2 = [](const nebula::surface::Accessor& r) {
    // std::string s;
    // if (!r.isNull("items")) {
    //   const auto list = r.readList("items");
    //   for (auto k = 0; k < list->getItems(); ++k) {
    //     s += fmt::format("{0},", list->readString(k));
    //   }
    // }

    return fmt::format("({0}, {1}, {2})",
                       r.readInt("id").value_or(0),
                       r.readString("event").value_or(""),
                       r.readBool("flag").value_or(false));
  };

  // do write
  {
    for (size_t i = 0, size = rows.size(); i < size; ++i) {
      batch.add(rows[i]);
    }

    // verify batch data
    EXPECT_EQ(batch.getRows(), count);

    // print out the batch state
    LOG(INFO) << "Batch: " << batch.state();
  }

  // do read verification
  {
    LOG(INFO) << "ROW 0: " << line(rows[0]);
    auto accessor = batch.makeAccessor();
    for (auto i = 0; i < count; ++i) {
      const auto& r1 = rows[i];
      const auto& r2 = accessor->seek(i);
      EXPECT_EQ(line(r1), line2(r2));
      // EXPECT_EQ(r1.readInt128("i128"), r2.readInt128("i128"));
    }

    LOG(INFO) << "Verified total rows: " << count;
  }
}

TEST(DataTreeTest, TestBuildDataTree) {
  nebula::meta::TestTable test;
  auto dataTree = nebula::memory::DataNode::buildDataTree(test, 10);
  EXPECT_EQ(dataTree->size(), test.schema()->size());
}

TEST(BatchTest, TestBloomFilter) {
  nebula::meta::TestTable test;
  int32_t count = 1000;

  // need some stable data set to write out and can be verified
  Batch batch(test, count);

  // use the specified seed so taht the data can repeat
  std::vector<nebula::surface::StaticRow> rows;
  // fill rows
  for (int32_t i = 0; i < count; ++i) {
    nebula::surface::StaticRow row{ i,
                                    i,
                                    "events",
                                    nullptr,
                                    false,
                                    0,
                                    0,
                                    0 };
    batch.add(row);
  }

  // check this batch has bloom filter on ID
  // assuming no false positive on this individual value
  // if the test becomes unstable, we can change the test
  auto falsePositives = 0;
  for (int32_t i = count; i < count * 2; ++i) {
    if (batch.probably("id", i)) {
      falsePositives++;
    }
  }

  // false positive rate should be less than 0.1%
  EXPECT_LT(falsePositives * 100.0 / count, 0.1f);
}

TEST(BatchTest, TestStringDictionary) {
  nebula::meta::TestTable test;
  int32_t count = 100000;

  // need some stable data set to write out and can be verified
  Batch batch(test, count);

  // use the specified seed so taht the data can repeat
  std::vector<nebula::surface::StaticRow> rows;
  rows.reserve(count);

  // fill rows
  int128_t large = nebula::common::Int128_U::UINT128_LOW_MASK + 256;
  for (int32_t i = 0; i < count; ++i) {
    nebula::surface::StaticRow row{ i,
                                    i,
                                    "nebula",
                                    nullptr,
                                    false,
                                    0,
                                    large,
                                    1.1 };
    batch.add(row);
    rows.push_back(row);
  }

  batch.seal();
  LOG(INFO) << "Batch State: " << batch.state();
  auto accessor = batch.makeAccessor();
  for (auto i = 0; i < count; ++i) {
    const auto& r1 = rows[i];
    const auto& r2 = accessor->seek(i);
    EXPECT_EQ(r1.readString("event"), r2.readString("event"));
    EXPECT_TRUE(r1.readInt128("stamp") == r2.readInt128("stamp"));
  }

  LOG(INFO) << "Verified total rows: " << count;
}

TEST(BatchTest, TestDefaultValue) {
  nebula::meta::TestTable test;
  int32_t count = 1000;

  // need some stable data set to write out and can be verified
  Batch batch(test, count);

  // use the specified seed so taht the data can repeat
  std::vector<nebula::surface::StaticRow> rows;
  // fill rows, even values will be null for "value" column
  // and it has default value as "23"
  for (int32_t i = 0; i < count; ++i) {
    nebula::surface::StaticRow row{ i,
                                    i,
                                    "events",
                                    nullptr,
                                    false,
                                    (char)(i % 32),
                                    128,
                                    1.1 };
    batch.add(row);
  }

  // check this batch has bloom filter on ID
  // assuming no false positive on this individual value
  // if the test becomes unstable, we can change the test
  auto accessor = batch.makeAccessor();
  for (auto i = 0; i < count; ++i) {
    const auto& row = accessor->seek(i);

    auto b = row.readByte("value");
    EXPECT_TRUE(b.has_value());
    int8_t expected = (int8_t)(i % 32);

    // even value will become default value
    // LOG(INFO) << "Value: " << (int)b;
    if (expected % 2 == 0) {
      EXPECT_EQ(b, 23);
    } else {
      EXPECT_EQ(b, expected);
    }
  }
}

TEST(BatchTest, TestPartitionedBatch) {
  nebula::meta::TestPartitionedTable test;
  size_t count = 10000;

  // need some stable data set to write out and can be verified
  // pid -> batch
  nebula::common::unordered_map<size_t, std::unique_ptr<Batch>> batches;

  // use the specified seed so taht the data can repeat
  std::vector<nebula::surface::StaticPartitionedRow> rows;
  MockRowData mr;
  // fill rows
  for (size_t i = 0; i < count; ++i) {
    rows.emplace_back(mr.readLong("_time_"),
                      mr.readByte("value"),
                      mr.readDouble("weight"));
  }

  // print single row as string.
  auto line = [](const nebula::surface::RowData& r) {
    return fmt::format("({0}, {1}, {2}, {3}, {4})",
                       r.readString("d1"), r.readByte("d2"), r.readInt("d3"),
                       r.readByte("value"), r.readDouble("weight"));
  };

  auto line2 = [](const nebula::surface::Accessor& r) {
    return fmt::format("({0}, {1}, {2}, {3}, {4})",
                       r.readString("d1").value_or(""),
                       r.readByte("d2").value_or(0),
                       r.readInt("d3").value_or(0),
                       r.readByte("value").value_or(0),
                       r.readDouble("weight").value_or(0));
  };

  // distribute to all batches
  std::vector<std::string> lines;
  lines.reserve(count);
  auto pod = test.pod();
  LOG(INFO) << "maximum blocks needed: " << pod->capacity();
  {
    for (size_t i = 0; i < count; ++i) {
      const auto& r = rows.at(i);
      // figure out which batch this row should go
      // using row major function defined in pod
      int32_t bess = -1;
      auto pid = pod->pod(r, bess);
      N_ENSURE(bess >= 0, "invalid bess");
      lines.emplace_back(line(r));
      auto itr = batches.find(pid);
      if (itr != batches.end()) {
        itr->second->add(r, bess);
      } else {
        auto p = new Batch(test, count, pid);
        auto b = std::unique_ptr<Batch>(p);
        b->add(r, bess);
        batches.emplace(pid, std::move(b));
      }
    }

    // sort all lines
    std::sort(lines.begin(), lines.end());

    std::vector<std::string> results;
    results.reserve(count);
    size_t totalRows = 0;
    for (auto& b : batches) {
      auto& batch = b.second;
      auto rows = batch->getRows();
      totalRows += rows;
      // print out the batch state
      LOG(INFO) << "Batch: " << batch->state();
      auto accessor = batch->makeAccessor();
      for (size_t i = 0; i < rows; ++i) {
        const auto& r = accessor->seek(i);
        results.emplace_back(line2(r));
      }
    }

    std::sort(results.begin(), results.end());
    // verify batch data
    EXPECT_EQ(totalRows, count);

    // all lines are the same
    EXPECT_EQ(lines, results);
  }
}

} // namespace test
} // namespace memory
} // namespace nebula