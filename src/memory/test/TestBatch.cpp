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
#include "common/Memory.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "memory/Batch.h"
#include "memory/DataNode.h"
#include "memory/FlatRow.h"
#include "surface/DataSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace memory {
namespace test {

using nebula::common::Evidence;
using nebula::surface::IndexType;
using nebula::surface::MockRowData;
using nebula::type::ROOT;
using nebula::type::TypeSerializer;

#define NOT_IMPL_FUNC(TYPE, NAME)             \
  TYPE NAME(IndexType index) const override { \
    throw NebulaException("x");               \
  }

class StaticList : public ListData {
public:
  StaticList(std::vector<std::string> data) : ListData(data.size()), data_{ std::move(data) } {
  }

  bool isNull(IndexType index) const override {
    return false;
  }

  std::string readString(IndexType index) const override {
    return data_.at(index);
  }

  NOT_IMPL_FUNC(bool, readBool)
  NOT_IMPL_FUNC(int8_t, readByte)
  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(int32_t, readInt)
  NOT_IMPL_FUNC(int64_t, readLong)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(double, readDouble)

private:
  std::vector<std::string> data_;
};

#undef NOT_IMPL_FUNC

#define NOT_IMPL_FUNC(TYPE, NAME)                      \
  TYPE NAME(const std::string& field) const override { \
    throw NebulaException("x");                        \
  }

// Mock static row for schema "ROW<id:int, event:string, items:list<string>, flag:bool>"
class StaticRow : public RowData {
public:
  StaticRow(int i, std::string s, std::unique_ptr<ListData> list, bool f)
    : id_{ i }, event_{ std::move(s) }, flag_{ f } {
    if (list != nullptr) {
      items_.reserve(list->getItems());
      for (auto k = 0; k < items_.capacity(); ++k) {
        items_.push_back(list->readString(k));
      }
    }
  }

  // All intrefaces - string type has RVO, copy elision optimization
  bool isNull(const std::string& field) const override {
    return field == "items" && items_.size() == 0;
  }

  bool readBool(const std::string& field) const override {
    return flag_;
  }

  int32_t readInt(const std::string& field) const override {
    return id_;
  }

  std::string readString(const std::string& field) const override {
    return event_;
  }

  std::unique_ptr<ListData> readList(const std::string& field) const override {
    return items_.size() == 0 ? nullptr : std::make_unique<StaticList>(items_);
  }

  NOT_IMPL_FUNC(int8_t, readByte)
  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(int64_t, readLong)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(double, readDouble)
  NOT_IMPL_FUNC(std::unique_ptr<MapData>, readMap)

private:
  int id_;
  std::string event_;
  std::vector<std::string> items_;
  bool flag_;
};

#undef NOT_IMPL_FUNC

TEST(BatchTest, TestBatch) {
  auto schema = TypeSerializer::from("ROW<id:int, items:list<string>, flag:bool>");
  Batch batch1(schema);
  Batch batch2(schema);

  // add 10 rows
  auto rows = 10;

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
  // need some stable data set to write out and can be verified
  auto schema = TypeSerializer::from("ROW<id:int, event:string, items:list<string>, flag:bool>");
  Batch batch(schema);

  // add 10 rows
  auto count = 1000;

  // use the specified seed so taht the data can repeat
  auto seed = Evidence::unix_timestamp();
  std::vector<StaticRow> rows;
  MockRowData row;
  // fill rows
  for (auto i = 0; i < count; ++i) {
    rows.push_back({ row.readInt("id"),
                     row.readString("event"),
                     i % 3 != 0 ? nullptr : row.readList("items"),
                     // row.isNull("items") ? nullptr : row.readList("items"),
                     row.readBool("flag") });
  }

  // print single row as string.
  auto line = [](const RowData& r) {
    std::string s;
    if (!r.isNull("items")) {
      const auto list = r.readList("items");
      for (auto k = 0; k < list->getItems(); ++k) {
        s += list->readString(k) + ",";
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
    for (auto i = 0; i < count; ++i) {
      const auto& r1 = rows[i];
      const auto& r2 = batch.row(i);
      // LOG(INFO) << "r1: " << line(r1);
      // LOG(INFO) << "r2: " << line(r2);
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
  auto schema = TypeSerializer::from("ROW<id:int, items:list<string>, flag:bool>");
  auto dataTree = nebula::memory::DataNode::buildDataTree(schema);
  EXPECT_EQ(dataTree->size(), 3);
}

} // namespace test
} // namespace memory
} // namespace nebula