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

#pragma once

#include <mutex>
#include <unordered_map>

#include "common/Hash.h"
#include "execution/io/BlockLoader.h"

// Table State is designed to describe table state in memory.
// It is supposed to be a tree type structure as:
// Table -> Spec -> Blocks
// Table State can rollup state all the way from leaf (blocks)
// It also provides management of real-data
namespace nebula {
namespace execution {

class TableStateBase {
public:
  using Window = std::pair<size_t, size_t>;
  TableStateBase(const std::string& table)
    : table_{ table },
      blocks_{ 0 },
      rows_{ 0 },
      bytes_{ 0 },
      window_{ std::numeric_limits<size_t>::max(), 0 } {}
  virtual ~TableStateBase() = default;

public:
  inline size_t numBlocks() const {
    return blocks_;
  }

  inline size_t numRows() const {
    return rows_;
  }

  inline size_t rawBytes() const {
    return bytes_;
  }

  inline std::pair<size_t, size_t> timeWindow() const {
    return window_;
  }

  inline const nebula::surface::eval::HistVector& hists() const {
    return hists_;
  }

  // merge metrics only from other table state object
  void merge(const TableStateBase& state, bool metricsOnly = true) {
    N_ENSURE(metricsOnly, "Only merge metrics only for now.");

    blocks_ += state.numBlocks();
    rows_ += state.numRows();
    bytes_ += state.rawBytes();
    window_.first = std::min(state.window_.first, window_.first);
    window_.second = std::max(state.window_.second, window_.second);

    // merge histogram
    merge(hists_, state.hists_);
  }

  static const TableStateBase& empty() {
    static const TableStateBase EMPTY{ "EMPTY" };
    return EMPTY;
  }

  inline static void merge(nebula::surface::eval::HistVector& target,
                           const nebula::surface::eval::HistVector& source) {
    // merge histogram from state into hists
    if (target.size() == 0) {
      target = source;
      return;
    }

    // TODO: need support for schema evolution
    N_ENSURE(target.size() == source.size(), "expect the same number of histograms");
    for (size_t i = 0; i < target.size(); ++i) {
      target.at(i)->merge(*source.at(i));
    }
  }

protected:
  // table name
  std::string table_;

  // num blocks
  size_t blocks_;
  // num rows
  size_t rows_;
  // total raw bytes of data in store
  size_t bytes_;
  // time window covers blocks
  Window window_;

  // column histogram
  nebula::surface::eval::HistVector hists_;
};

// a shortcut for pair set of {table name, spec id}
using TableSpecSet = nebula::common::unordered_set<std::pair<std::string, std::string>>;

// Table State with solid data in it
class TableState : public TableStateBase {
public:
  TableState(const std::string& table) : TableStateBase(table) {}
  virtual ~TableState() = default;
  inline bool hasSpec(const std::string& spec) const {
    return data_.find(spec) != data_.end();
  }

  TableSpecSet expired(std::function<bool(const std::string&, const std::string&)> shouldExpire) const {
    TableSpecSet specs;
    const std::lock_guard<std::mutex> lock(mdata_);
    for (auto& b : data_) {
      // assign existing spec, expire it if not assigned
      if (shouldExpire(table_, b.first)) {
        specs.emplace(table_, b.first);
      }
    }

    return specs;
  }

public:
  // add a block already loaded
  bool add(std::shared_ptr<nebula::execution::io::BatchBlock>);

  // remove all blocks for given spec
  size_t remove(const std::string&);

  // get all data batch pointers by given window
  std::vector<nebula::memory::BatchPtr> query(const Window&) const;

  // iterate every single block to feed the given lambda
  void iterate(std::function<void(const nebula::execution::io::BatchBlock&)>) const;

private:
  // spec signature -> multi blocks
  std::unordered_multimap<std::string, std::shared_ptr<nebula::execution::io::BatchBlock>> data_;
  mutable std::mutex mdata_;
};

} // namespace execution
} // namespace nebula