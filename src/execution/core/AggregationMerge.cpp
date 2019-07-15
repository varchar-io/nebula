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
#include "execution/eval/UDF.h"
#include "memory/keyed/FlatRowCursor.h"

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
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::type::Kind;
using nebula::type::Schema;

RowCursorPtr merge(
  const Schema schema,
  const std::vector<size_t>& keys,
  const std::vector<std::unique_ptr<ValueEval>>& fields,
  const bool hasAggregation,
  const std::vector<folly::Try<nebula::surface::RowCursorPtr>>& sources) {
  LOG(INFO) << fmt::format("Merge sources: {0} with aggregation: {1}", sources.size(), hasAggregation);

  if (hasAggregation) {
#define P_FIELD_UPDATE(KIND, TYPE)                                                                                \
  case Kind::KIND: {                                                                                              \
    auto p = static_cast<TYPE*>(value);                                                                           \
    *p = static_cast<UDAF<Kind::KIND>&>(*fields[column]).merge(*static_cast<TYPE*>(ov), *static_cast<TYPE*>(nv)); \
    return true;                                                                                                  \
  }
    // build the runtime data set
    auto hf = std::make_unique<HashFlat>(schema, keys);

    for (auto it = sources.begin(); it < sources.end(); ++it) {
      auto blockResult = it->value();

      // iterate the block result and partial aggregate it in hash flat
      while (blockResult->hasNext()) {
        const auto& row = blockResult->next();
        hf->update(row, [&fields](size_t column, Kind kind, void* ov, void* nv, void* value) {
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
        });
      }
    }

    // wrap the hash flat into a row cursor
    return std::make_shared<FlatRowCursor>(std::move(hf));
#undef P_FIELD_UPDATE
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