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

#include "execution/ExecutionPlan.h"
#include "memory/Batch.h"
#include "surface/SchemaRow.h"
#include "surface/eval/ValueEval.h"

namespace nebula {
namespace execution {
namespace core {

// computed row use index based interfaces rather than name based interface.
class ComputedRow : public nebula::surface::SchemaRow {
  using IndexType = nebula::surface::IndexType;

public:
  ComputedRow(const nebula::surface::Name2Index& fieldMap,
              const nebula::surface::eval::Fields& fields,
              std::shared_ptr<nebula::surface::eval::EvalContext> ctx)
    : SchemaRow(fieldMap), fields_{ fields }, ctx_{ ctx } {}
  virtual ~ComputedRow() = default;

public:
  bool isNull(IndexType) const override;
  bool readBool(IndexType) const override;
  int8_t readByte(IndexType) const override;
  int16_t readShort(IndexType) const override;
  int32_t readInt(IndexType) const override;
  int64_t readLong(IndexType) const override;
  float readFloat(IndexType) const override;
  double readDouble(IndexType) const override;
  int128_t readInt128(IndexType) const override;
  std::string_view readString(IndexType) const override;

  // compound types
  std::unique_ptr<nebula::surface::ListData> readList(IndexType) const override;
  std::unique_ptr<nebula::surface::MapData> readMap(IndexType) const override;

private:
  const nebula::surface::eval::Fields& fields_;

  // evaluation context and wrapped row
  std::shared_ptr<nebula::surface::eval::EvalContext> ctx_;

  // TODO(cao) - we have been struggling on whether to use std::optional as RowData interface
  // There are basically two situations for providing RowData interface:
  // 1. the RowData interface is served upon a stateful data set,
  //    which we can easily extract NULL state before invoking readX interface to get value
  //    Batch, FlatBuffer or FlatRow are all these cases
  // 2. Another case is serving RowData interface on top of a runtime.
  //    In this case, we don't know its null state before we finish the computing and we don't want to compute twice.
  //    ComputedRow, Parquet/Kafka reader are all these cases, in thse cases, today we have to use a "cache" to store its state to serve both API.
  // Accordingly, we definitely have multiple choices to mitigate this issue:
  // 1. we can unify isNull API into single readX API with a return type of std::optional<>
  //    this solution provides clean and nice interface in the whole system, the downside of it is the overhead of memory layout,
  //    as the wrapper introduces more bytes per type especially when layout alignment is enforced. Are we willing to pay 16 bytes for storing a double value
  //    We may look for faster/simpler version to replace it such as compact_optional<> or markable (https://github.com/akrzemi1/markable)
  // 2. build fast cache to bridge isNull and readX as what it is, like how can FlatRow fast enough?
};

} // namespace core
} // namespace execution
} // namespace nebula