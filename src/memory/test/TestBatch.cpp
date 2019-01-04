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
#include "FlatRow.h"
#include "Memory.h"
#include "Serde.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace nebula {
namespace memory {
namespace test {

using nebula::type::ROOT;
using nebula::type::TypeSerializer;

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

} // namespace test
} // namespace memory
} // namespace nebula