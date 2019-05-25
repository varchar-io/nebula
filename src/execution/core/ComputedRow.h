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

#include "execution/eval/ValueEval.h"
#include "surface/SchemaRow.h"

namespace nebula {
namespace execution {
namespace core {

// TODO(cao) - we should remove this constructor.
// Feels expensive to construct every one for each row.
// computed row use index based interfaces rather than name based interface.
class ComputedRow : public nebula::surface::SchemaRow {
  using IndexType = nebula::surface::IndexType;

public:
  ComputedRow(
    const nebula::type::Schema& schema,
    nebula::execution::eval::EvalContext& ctx,
    const std::vector<std::unique_ptr<eval::ValueEval>>& fields)
    : SchemaRow(schema), ctx_{ ctx }, fields_{ fields } {}
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
  std::string_view readString(IndexType) const override;

  // compound types
  std::unique_ptr<nebula::surface::ListData> readList(IndexType) const override;
  std::unique_ptr<nebula::surface::MapData> readMap(IndexType) const override;

private:
  nebula::execution::eval::EvalContext& ctx_;
  const std::vector<std::unique_ptr<eval::ValueEval>>& fields_;
};

} // namespace core
} // namespace execution
} // namespace nebula