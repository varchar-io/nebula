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

#include "gtest/gtest.h"
#include <glog/logging.h>
#include <valarray>
#include "common/Memory.h"
#include "fmt/format.h"
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
  MockRowData row;
  // fill rows
  for (auto i = 0; i < count; ++i) {
    rows.push_back({ row.readLong("_time_"),
                     row.readInt("id"),
                     row.readString("event"),
                     i % 3 != 0 ? nullptr : row.readList("items"),
                     // row.isNull("items") ? nullptr : row.readList("items"),
                     row.readBool("flag"),
                     row.readByte("value") });
  }

  // print single row as string.
  auto line = [](const nebula::surface::RowData& r) {
    std::string s;
    if (!r.isNull("items")) {
      const auto list = r.readList("items");
      for (auto k = 0; k < list->getItems(); ++k) {
        s += fmt::format("{0},", list->readString(k));
      }
    }

    return fmt::format("({0}, {1}, [{2}], {3})",
                       r.readInt("id"), r.readString("event"), s, r.readBool("flag"));
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
      EXPECT_EQ(line(r1), line(r2));
    }

    LOG(INFO) << "Verified total rows: " << count;
  }
}

TEST(RowTest, TestFlatRow) {
  // should be a covariant of TypeNode shared_ptr<RowType> -> shared_ptr<TypeBase>
  auto schema = TypeSerializer::from("ROW<id:int, items:list<string>, flag:bool>");

  // initialize a flat row with given schema
  FlatRow row(schema);

  // to achieve flexible writing APIs
  // here is a contract for each type
  // 1. list - begin and write
  // 2. struct - all field writing need specifying name
  // 3. map - treat it like struct but with two special field name [KEY], [VALUE]
  // Before write(value) is called, the expected node should be placed in the stack

  // write id as value 2
  row.write("id", 2);
  auto fItems = row.locate("items");
  // start 3 items
  row.begin(fItems, 3);
  row.write("x");
  row.write("y");
  row.write("z");
  row.end(fItems);
  // write flag as false
  row.write("flag", false);

  // end root for reading/serailization
  row.seal();

  // row is ready to read now
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

} // namespace test
} // namespace memory
} // namespace nebula