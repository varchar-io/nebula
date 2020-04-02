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

#pragma once

#include <forward_list>
#include <glog/logging.h>

#include "memory/Batch.h"
#include "meta/NBlock.h"
#include "meta/TestTable.h"

/**
 * This node executor accepts one request of an execution plan.
 * And starts to conduct block scan and partial agg operations and return results to the requester.
 */
namespace nebula {
namespace execution {
namespace io {

using BatchBlock = nebula::meta::NBlock<nebula::memory::Batch>;
using BlockList = std::forward_list<BatchBlock>;

// load a NBlock into memory
class BlockLoader {
public:
  static BatchBlock from(const nebula::meta::BlockSignature&, std::shared_ptr<nebula::memory::Batch>);

public:
  BlockList load(const nebula::meta::BlockSignature&);

private:
  // Test Hook
  BlockList loadTestBlock(const nebula::meta::BlockSignature&);
  nebula::meta::TestTable test_;
};

} // namespace io
} // namespace execution
} // namespace nebula