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

#include "Test.hpp"

#include "api/dsl/Serde.h"
#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "meta/NBlock.h"
#include "meta/TestTable.h"

namespace nebula {
namespace api {
namespace test {

using nebula::api::dsl::CustomColumn;
using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::meta::BlockSignature;

std::mutex& sync_test_mutex() {
  static std::mutex exclusive;
  return exclusive;
}

std::tuple<std::string, int64_t, int64_t> genData(unsigned numBlocks) {
  numBlocks /= 2;
  static std::tuple<std::string, int64_t, int64_t> data{ "", 0, 0 };

  // global mutex
  std::lock_guard<std::mutex> lock(sync_test_mutex());
  if (std::get<1>(data) != 0) {
    return data;
  }

  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  int64_t start = Evidence::time("2019-01-01", "%Y-%m-%d");
  int64_t end = Evidence::time("2019-05-01", "%Y-%m-%d");

  // let's plan these many data std::thread::hardware_concurrency()
  nebula::meta::TestTable testTable;

  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add(BlockSignature{ testTable.name(), i, begin, begin + window });
  }

  data = { testTable.name(), start, end };
  return data;
}

} // namespace test
} // namespace api
} // namespace nebula