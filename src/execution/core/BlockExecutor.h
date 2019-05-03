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

#include "execution/ExecutionPlan.h"
#include "memory/Batch.h"
#include "memory/keyed/HashFlat.h"
#include "surface/DataSurface.h"

/**
 * Block executor is used to apply computing on a single block
 */
namespace nebula {
namespace execution {
namespace core {

/**
 * Block executor defines the smallest compute unit and itself is a row data cursor
 */
class BlockExecutor : public nebula::common::Cursor<nebula::surface::RowData> {
  using Base = nebula::common::Cursor<nebula::surface::RowData>;

public:
  BlockExecutor(const nebula::memory::Batch& data, const nebula::execution::BlockPhase& plan)
    : Base(0), data_{ data }, plan_{ plan } {
    // compute will finish the compute and fill the data state in
    this->compute();
  }
  virtual ~BlockExecutor() = default;

  virtual const nebula::surface::RowData& next() override;
  virtual std::unique_ptr<nebula::surface::RowData> item(size_t) const override;

private:
  void compute();

private:
  const nebula::memory::Batch& data_;
  const nebula::execution::BlockPhase& plan_;
  std::unique_ptr<nebula::memory::keyed::HashFlat> result_;
};

// computed row use index based interfaces rather than name based interface.
class ComputedRow : public nebula::surface::RowData {
  using IndexType = nebula::surface::IndexType;

public:
  ComputedRow(const nebula::surface::RowData& input, const std::vector<std::unique_ptr<eval::ValueEval>>& fields)
    : input_{ input }, fields_{ fields } {
  }
  virtual ~ComputedRow() = default;

public:
#define NOT_IMPL_FUNC(TYPE, FUNC)                  \
  TYPE FUNC(const std::string&) const override {   \
    throw NException(#FUNC " is not implemented"); \
  }

  NOT_IMPL_FUNC(bool, readBool)
  NOT_IMPL_FUNC(int8_t, readByte)
  NOT_IMPL_FUNC(int16_t, readShort)
  NOT_IMPL_FUNC(int32_t, readInt)
  NOT_IMPL_FUNC(int64_t, readLong)
  NOT_IMPL_FUNC(float, readFloat)
  NOT_IMPL_FUNC(double, readDouble)
  NOT_IMPL_FUNC(std::string, readString)
  NOT_IMPL_FUNC(std::unique_ptr<nebula::surface::ListData>, readList)
  NOT_IMPL_FUNC(std::unique_ptr<nebula::surface::MapData>, readMap)

#undef NOT_IMPL_FUNC

  bool isNull(const std::string& field) const override;
  bool isNull(IndexType) const override;
  bool readBool(IndexType) const override;
  int8_t readByte(IndexType) const override;
  int16_t readShort(IndexType) const override;
  int32_t readInt(IndexType) const override;
  int64_t readLong(IndexType) const override;
  float readFloat(IndexType) const override;
  double readDouble(IndexType) const override;
  std::string readString(IndexType) const override;

  // compound types
  std::unique_ptr<nebula::surface::ListData> readList(IndexType) const override;
  std::unique_ptr<nebula::surface::MapData> readMap(IndexType) const override;

private:
  const nebula::surface::RowData& input_;
  const std::vector<std::unique_ptr<eval::ValueEval>>& fields_;
};

} // namespace core
} // namespace execution
} // namespace nebula