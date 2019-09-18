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

#include "AggregationMerge.h"

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "BlockExecutor.h"
#include "common/Fold.h"
#include "execution/eval/UDF.h"
#include "memory/keyed/FlatRowCursor.h"

DEFINE_uint64(MULTI_FOLD_WIDTH, 0,
              "defines if we want fixed width for multi-fold passes."
              "If less than 2, width will be decided automatically by pool size.");

/**
 * A logic wrapper to merge aggregation results shared by aggregators (Node Executor or Server Executor)
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::common::CompositeCursor;
using nebula::execution::eval::UDAF;
using nebula::execution::eval::ValueEval;
using nebula::memory::keyed::FlatRowCursor;
using nebula::memory::keyed::HashFlat;
using nebula::surface::EmptyRowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::type::Kind;
using nebula::type::Schema;

RowCursorPtr merge(
  folly::ThreadPoolExecutor& pool,
  const std::vector<std::unique_ptr<ValueEval>>& fields,
  const bool hasAggregation,
  const std::vector<folly::Try<nebula::surface::RowCursorPtr>>& sources) {
  const auto size = sources.size();
  LOG(INFO) << fmt::format("Merge sources: {0} with aggregation: {1}", size, hasAggregation);
  if (size == 0) {
    LOG(INFO) << "Received an empty result set.";
    return EmptyRowCursor::instance();
  }

  if (hasAggregation) {
#define P_FIELD_UPDATE(KIND, TYPE)                                                                                \
  case Kind::KIND: {                                                                                              \
    auto p = static_cast<TYPE*>(value);                                                                           \
    *p = static_cast<UDAF<Kind::KIND>&>(*fields[column]).merge(*static_cast<TYPE*>(ov), *static_cast<TYPE*>(nv)); \
    return true;                                                                                                  \
  }

    auto update = [&fields](size_t column, Kind kind, void* ov, void* nv, void* value) {
      // others are aggregation fields - aka, they are UDAF
      switch (kind) {
        P_FIELD_UPDATE(BOOLEAN, bool)
        P_FIELD_UPDATE(TINYINT, int8_t)
        P_FIELD_UPDATE(SMALLINT, int16_t)
        P_FIELD_UPDATE(INTEGER, int32_t)
        P_FIELD_UPDATE(REAL, float)
        P_FIELD_UPDATE(BIGINT, int64_t)
        P_FIELD_UPDATE(DOUBLE, double)
      default:
        throw NException("Aggregation not supporting non-primitive types");
      }
    };
#undef P_FIELD_UPDATE

    // if it is aggregation, we're sure the data cursor will be hash flat.
    // So that we can do this multi-fold pass
    // transform folly tries into HashFlat
    std::vector<std::unique_ptr<HashFlat>> blocks;
    blocks.reserve(size);
    for (auto it = sources.begin(); it < sources.end(); ++it) {
      // if the result is empty
      if (!it->hasValue()) {
        continue;
      }

      auto b = std::static_pointer_cast<nebula::execution::core::BlockExecutor>(it->value());
      auto buffer = b->takeResult();
      auto flat = std::unique_ptr<HashFlat>{ static_cast<HashFlat*>(buffer.release()) };
      blocks.push_back(std::move(flat));
    }

    if (blocks.size() == 0) {
      LOG(INFO) << "no valid block to merge.";
      return EmptyRowCursor::instance();
    }

    // fold blocks if there are more than one
    if (blocks.size() > 1) {
      nebula::common::fold<std::unique_ptr<HashFlat>>(
        pool, blocks, [&update](std::unique_ptr<HashFlat>& from, std::unique_ptr<HashFlat>& to) {
          // do the in-place merge
          // iterate the block result and partial aggregate it in hash flat
          FlatRowCursor cursor(std::move(from));
          while (cursor.hasNext()) {
            const auto& row = cursor.next();
            to->update(row, update);
          }
        },
        FLAGS_MULTI_FOLD_WIDTH);
    }

    // after fold, all data aggregated at position 0
    return std::make_shared<FlatRowCursor>(std::move(blocks.at(0)));
  }

  auto composite = std::make_shared<CompositeCursor<RowData>>();
  auto failures = 0;
  for (auto it = sources.begin(); it < sources.end(); ++it) {
    if (it->hasValue()) {
      auto& cursor = it->value();
      if (cursor) {
        composite->combine(cursor);
        continue;
      }
    }

    ++failures;
  }

  // We may fail depends on scenario / requirement (use-case dependant)
  if (failures > 0) {
    LOG(INFO) << "Error or timeout nodes: " << failures;
  }

  return composite;
}

} // namespace core
} // namespace execution
} // namespace nebula