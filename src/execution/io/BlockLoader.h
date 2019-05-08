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

#include <glog/logging.h>
#include "memory/Batch.h"
#include "meta/NBlock.h"

/**
 * This node executor accepts one request of an execution plan.
 * And starts to conduct block scan and partial agg operations and return results to the requester.
 */
namespace nebula {
namespace execution {
namespace io {
// load a NBlock into memory
class BlockLoader {
public:
  std::unique_ptr<nebula::memory::Batch> load(const nebula::meta::NBlock&);

private:
  // Test Hook
  std::unique_ptr<nebula::memory::Batch> loadTestBlock(const nebula::meta::NBlock&);

  // Trends Data Hook
  std::unique_ptr<nebula::memory::Batch> loadTrendsBlock(const nebula::meta::NBlock&);
};

} // namespace io
} // namespace execution
} // namespace nebula