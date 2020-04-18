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

#include "TableState.h"

namespace nebula {
namespace execution {

using nebula::execution::io::BatchBlock;
using nebula::meta::BlockSignature;

// update block metrics to a list of variables
inline void update(const BatchBlock& block, size_t& rows, size_t& bytes, std::pair<size_t, size_t>& window) {
  const auto& state = block.state();
  rows += state.numRows;
  bytes += state.rawSize;
  window.first = std::min(block.start(), window.first);
  window.second = std::max(block.end(), window.second);
}

void TableState::merge(const TableState& state, bool metricsOnly) {
  N_ENSURE(metricsOnly, "Only merge metrics only for now.");

  blocks_ += state.numBlocks();
  rows_ += state.numRows();
  bytes_ += state.rawBytes();
  window_.first = std::min(state.window_.first, window_.first);
  window_.second = std::max(state.window_.second, window_.second);
}

void TableState::iterate(std::function<void(const BatchBlock&)> func) const {
  for (auto& b : data_) {
    func(*b.second);
  }
}

bool TableState::add(std::shared_ptr<BatchBlock> block) {
  const auto& spec = block->spec();

  // collect metrics
  update(*block, rows_, bytes_, window_);
  ++blocks_;

  // add this block to the repo
  data_.emplace(spec, block);
  return true;
}

size_t TableState::remove(const std::string& spec) {
  auto count = data_.erase(spec);

  // update the metrics
  size_t rows = 0;
  size_t bytes = 0;
  std::pair<size_t, size_t> window{ std::numeric_limits<size_t>::max(), 0 };
  for (auto& b : data_) {
    update(*b.second, rows, bytes, window);
  }

  blocks_ = data_.size();
  rows_ = rows;
  bytes_ = bytes;
  window_ = window;

  return count;
}

std::vector<nebula::memory::BatchPtr> TableState::query(const Window& window) const {
  std::vector<nebula::memory::BatchPtr> batches;
  batches.reserve(data_.size());

  for (auto& b : data_) {
    if (b.second->overlap(window)) {
      batches.push_back(b.second->data());
    }
  }

  return batches;
}

} // namespace execution
} // namespace nebula