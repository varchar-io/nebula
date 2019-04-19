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

#include "BlockExecutor.h"
#include <unordered_set>
#include "execution/eval/UDF.h"
#include "memory/keyed/HashFlat.h"

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::execution::eval::UDAF;
using nebula::execution::eval::ValueEval;
using nebula::memory::keyed::HashFlat;
using nebula::surface::RowData;
using nebula::type::Kind;

void BlockExecutor::compute() {
  // process every single row and put result in HashFlat
  const auto& k = plan_.keys();
  std::unordered_set<size_t> keys(k.begin(), k.end());

  //plan_.fields()
  // build the runtime data set
  result_ = std::make_unique<HashFlat>(plan_.outputSchema(), k);
  auto accessor = data_.makeAccessor();
  const auto& fields = plan_.fields();
  const auto& filter = plan_.filter();

#define AGG_FIELD_UPDATE(KIND, TYPE)                                                                                                    \
  case Kind::KIND: {                                                                                                                    \
    *static_cast<TYPE*>(value) = static_cast<UDAF<Kind::KIND>&>(*fields[column]).agg(*static_cast<TYPE*>(ov), *static_cast<TYPE*>(nv)); \
    return true;                                                                                                                        \
  }

  for (size_t i = 0, size = data_.getRows(); i < size; ++i) {
    const RowData& row = accessor->seek(i);

    // if not fullfil the condition
    if (!filter.eval<bool>(row)) {
      continue;
    }

    // flat compute every new value of each field and set to corresponding column in flat
    auto cr = ComputedRow(row, fields);
    result_->update(cr, [&fields, &keys](size_t column, Kind kind, void* ov, void* nv, void* value) {
      // we don't update keys since they are the same
      if (keys.find(column) != keys.end()) {
        return false;
      }

      // others are aggregation fields - aka, they are UDAF
      switch (kind) {
        AGG_FIELD_UPDATE(BOOLEAN, bool)
        AGG_FIELD_UPDATE(TINYINT, int8_t)
        AGG_FIELD_UPDATE(SMALLINT, int16_t)
        AGG_FIELD_UPDATE(INTEGER, int32_t)
        AGG_FIELD_UPDATE(REAL, float)
        AGG_FIELD_UPDATE(BIGINT, int64_t)
        AGG_FIELD_UPDATE(DOUBLE, double)
      default:
        throw NException("Aggregation not supporting non-primitive types");
      }
    });
  }

#undef AGG_FIELD_UPDATE

  // after the compute flat should contain all the data we need.
  index_ = 0;
  size_ = result_->getRows();
}

const RowData& BlockExecutor::next() {
  return result_->row(index_++);
}

bool ComputedRow::isNull(const std::string&) const {
  // TODO(cao): how to determine nullbility of output field?
  return false;
}

bool ComputedRow::isNull(IndexType) const {
  // TODO(cao): how to determine nullbility of output field?
  return false;
}

#define FORWARD_EVAL_FIELD(TYPE, NAME)            \
  TYPE ComputedRow::NAME(IndexType index) const { \
    return fields_[index]->eval<TYPE>(input_);    \
  }

FORWARD_EVAL_FIELD(bool, readBool)
FORWARD_EVAL_FIELD(int8_t, readByte)
FORWARD_EVAL_FIELD(int16_t, readShort)
FORWARD_EVAL_FIELD(int32_t, readInt)
FORWARD_EVAL_FIELD(int64_t, readLong)
FORWARD_EVAL_FIELD(float, readFloat)
FORWARD_EVAL_FIELD(double, readDouble)
FORWARD_EVAL_FIELD(std::string, readString)

#undef FORWARD_EVAL_FIELD

std::unique_ptr<nebula::surface::ListData> ComputedRow::readList(IndexType) const {
  throw NException("Not implemented yet");
}

std::unique_ptr<nebula::surface::MapData> ComputedRow::readMap(IndexType) const {
  throw NException("Not implemented yet");
}

} // namespace core
} // namespace execution
} // namespace nebula