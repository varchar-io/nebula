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
#include <valarray>
#include "Batch.h"
#include "DataNode.h"
#include "DataSurface.h"
#include "FlatRow.h"
#include "Memory.h"
#include "Serde.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace nebula {
namespace memory {
namespace test {

using nebula::surface::MockRowData;
using nebula::type::ROOT;
using nebula::type::TypeSerializer;

TEST(BatchTest, TestBatch) {
  auto schema = TypeSerializer::from("ROW<id:int, items:list<string>, flag:bool>");
  Batch batch(schema);
  // add 10 rows
  auto rows = 10;
  MockRowData row;
  for (auto i = 0; i < rows; ++i) {
    batch.add(row);
  }

  // verify batch data
  EXPECT_EQ(batch.getRows(), rows);

  // every row is the same, they are
  auto id = row.readInt("id");
  auto items = row.readList("items");
  auto flag = row.readBool("flag");

  for (auto i = 0; i < rows; ++i) {
    // row cursor
    const RowData& cursor = batch.row(i);

    // check all column data
    EXPECT_EQ(cursor.readInt("id"), id);

    auto list = cursor.readList("items");
    EXPECT_EQ(list->getItems(), items->getItems());
    for (auto k = 0; k < items->getItems(); ++k) {
      EXPECT_EQ(list->readString(k), items->readString(k));
    }

    EXPECT_EQ(cursor.readBool("flag"), flag);
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
  auto schema = TypeSerializer::from("ROW<id:int, items:list<string>, flag:bool>");
  auto dataTree = nebula::memory::DataNode::buildDataTree(schema);
  EXPECT_EQ(dataTree->size(), 3);
}

} // namespace test
} // namespace memory
} // namespace nebula