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

#include "TableState.h"

namespace nebula {
namespace execution {

using nebula::execution::io::BatchBlock;
using nebula::memory::BatchPtr;
using BlockPtr = std::shared_ptr<BatchBlock>;
using nebula::surface::eval::HistVector;

#define LOCK_DATA_ACCESS const std::lock_guard<std::mutex> lock(mdata_);

// update block metrics to a list of variables
inline void update(const BlockPtr block,
                   size_t& rows,
                   size_t& bytes,
                   TableState::Window& window,
                   HistVector& hists) {
  const auto& state = block->state();
  rows += state.numRows;
  bytes += state.rawSize;
  window.first = std::min(block->start(), window.first);
  window.second = std::max(block->end(), window.second);
  TableStateBase::merge(hists, state.histograms);
}

void TableState::iterate(std::function<void(const nebula::execution::io::BatchBlock& block)> func) const {
  const std::lock_guard<std::mutex> lock(mdata_);
  for (auto& b : data_) {
    func(*b.second);
  }
}

bool TableState::add(std::shared_ptr<BatchBlock> block) {
  LOCK_DATA_ACCESS

  const auto& spec = block->spec();

  // collect metrics
  update(block, rows_, bytes_, window_, hists_);
  ++blocks_;

  // add this block to the repo
  data_.emplace(spec, block);
  return true;
}

size_t TableState::remove(const std::string& spec) {
  LOCK_DATA_ACCESS

  auto count = data_.erase(spec);

  // update the metrics
  size_t rows = 0;
  size_t bytes = 0;
  std::pair<int64_t, int64_t> window{ std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::min() };
  HistVector hists;
  for (auto& b : data_) {
    update(b.second, rows, bytes, window, hists);
  }

  // updated state data
  blocks_ = data_.size();
  rows_ = rows;
  bytes_ = bytes;
  std::swap(window_, window);
  std::swap(hists_, hists);

  return count;
}

std::vector<BatchPtr> TableState::query(const Window& window, const std::string& version) const {
  LOCK_DATA_ACCESS

  std::vector<BatchPtr> batches;
  batches.reserve(data_.size());

  for (auto& b : data_) {
    if (b.second->overlap(window) && b.second->version() == version) {
      batches.push_back(b.second->data());
    }
  }

  return batches;
}

#undef LOCK_DATA_ACCESS

} // namespace execution
} // namespace nebula