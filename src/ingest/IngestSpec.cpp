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

#include "IngestSpec.h"

#include <gflags/gflags.h>

#include "common/Evidence.h"
#include "execution/BlockManager.h"
#include "meta/TestTable.h"

DEFINE_string(NTEST_LOADER, "NebulaTest", "define the loader name for loading nebula test data");
/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace ingest {

using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::meta::TableSpecPtr;
using nebula::meta::TestTable;

// load some nebula test data into current process
void loadNebulaTestData(const TableSpecPtr& table) {
  // load test data to run this query
  auto bm = BlockManager::init();

  // set up a start and end time for the data set in memory
  auto start = table->timeSpec.unixTimeValue;
  auto end = start + Evidence::HOUR_SECONDS * table->max_hr;

  // let's plan these many data std::thread::hardware_concurrency()
  TestTable testTable;
  auto numBlocks = std::thread::hardware_concurrency();
  auto window = (end - start) / numBlocks;
  for (unsigned i = 0; i < numBlocks; i++) {
    size_t begin = start + i * window;
    bm->add({ testTable.name(), i++, begin, begin + window });
  }
}

bool IngestSpec::work() noexcept {
  // TODO(cao) - refator this to have better hirachy for different ingest types.
  if (table_->loader == FLAGS_NTEST_LOADER) {
    loadNebulaTestData(table_);
    return true;
  }

  return false;
}

} // namespace ingest
} // namespace nebula