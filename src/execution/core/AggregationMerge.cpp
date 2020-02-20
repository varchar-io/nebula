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

#include "common/Fold.h"
#include "execution/serde/RowCursorSerde.h"
#include "memory/keyed/FlatRowCursor.h"
#include "memory/keyed/HashFlat.h"
#include "surface/eval/UDF.h"

DEFINE_uint64(MULTI_FOLD_WIDTH, 1,
              "defines if we want fixed width for multi-fold passes."
              "0: use pool size to calculate width = total / size"
              "1: use current thread, not using pool"
              "2+: use this width to do folding parallel");

/**
 * A logic wrapper to merge aggregation results shared by aggregators (Node Executor or Server Executor)
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::common::CompositeCursor;
using nebula::memory::keyed::FlatRowCursor;
using nebula::memory::keyed::HashFlat;
using nebula::surface::EmptyRowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::surface::eval::ValueEval;
using nebula::type::Kind;
using nebula::type::Schema;

RowCursorPtr merge(
  folly::ThreadPoolExecutor&,
  const Schema schema,
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
    // if it is aggregation, we're sure the data cursor will be hash flat.
    // So that we can do this multi-fold pass
    // transform folly tries into HashFlat
    // std::vector<std::unique_ptr<HashFlat>> blocks;
    // blocks.reserve(size);
    auto hf = std::make_unique<HashFlat>(schema, fields);
    for (auto it = sources.begin(); it < sources.end(); ++it) {
      // if the result is empty
      if (!it->hasValue()) {
        continue;
      }

      auto blockResult = it->value();
      while (blockResult->hasNext()) {
        const auto& row = blockResult->next();
        hf->update(row);
      }
    }

    return std::make_shared<FlatRowCursor>(std::move(hf));
  }

  // TODO(cao) - I'm seeing multi-fold problems and even worse performance
  // Need more time to work on this, most likely caused by the manipulation on the pointers/casting/moving
  //   // convert to flat buffer pointer, we do not need schema used for new instance
  //   auto buffer = nebula::execution::serde::asBuffer(*it->value(), nullptr);
  //   N_ENSURE_NOT_NULL(buffer, "invalid buffer in merge path");
  //   // this buffer could be HashFlat if it is from local aggregation
  //   // or plain flat buffer which is searialized through boundary
  //   auto origin = buffer.release();
  //   auto flat = dynamic_cast<HashFlat*>(origin);
  //   if (flat) {
  //     blocks.push_back(std::unique_ptr<HashFlat>{ flat });
  //   } else {
  //     blocks.push_back(std::make_unique<HashFlat>(origin, keys));
  //   }
  // }

  // if (blocks.size() == 0) {
  //   LOG(INFO) << "no valid block to merge.";
  //   return EmptyRowCursor::instance();
  // }

  // fold blocks if there are more than one
  // if (blocks.size() > 1) {
  //   nebula::common::fold<std::unique_ptr<HashFlat>>(
  //     pool, blocks, [&update](std::unique_ptr<HashFlat>& from, std::unique_ptr<HashFlat>& to) {
  //       // do the in-place merge
  //       // iterate the block result and partial aggregate it in hash flat
  //       FlatRowCursor cursor(std::move(from));
  //       auto i = 0;
  //       while (cursor.hasNext()) {
  //         const auto& row = cursor.next();
  //         to->update(row, update);
  //       }
  //     },
  //     FLAGS_MULTI_FOLD_WIDTH);
  // }

  // // after fold, all data aggregated at position 0
  // return std::make_shared<FlatRowCursor>(std::move(blocks.at(0)));

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