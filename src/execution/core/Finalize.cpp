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

#include "Finalize.h"
#include <gflags/gflags.h>

#include "memory/FlatRow.h"
#include "surface/DataSurface.h"
#include "surface/SchemaRow.h"
#include "surface/eval/UDF.h"
#include "type/Type.h"

DEFINE_uint64(FOWARD_ROW_CACHE_SIZE,
              256,
              "bytes per slice o use for caching forward row");

/**
 * A logic wrapper to return top sort cursors when sorting and limiting are present
 */
namespace nebula {
namespace execution {
namespace core {

using nebula::memory::FlatRow;
using nebula::surface::IndexType;
using nebula::surface::RowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::RowData;
using nebula::surface::SchemaRow;
using nebula::surface::eval::Aggregator;
using nebula::surface::eval::Sketch;
using nebula::surface::eval::UDAF;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::TypeTraits;

// transformers to be executed for each column if it needs a transformer
using TransformerVector = std::vector<std::function<void(const RowData*, void*)>>;

class ForwardRowData : public nebula::surface::SchemaRow {
public:
  ForwardRowData(const nebula::type::Schema& input,
                 const TransformerVector& transformers)
    : SchemaRow(input),
      inner_{ nullptr },
      row_{ nullptr },
      transformers_{ transformers },
      values_{ FLAGS_FOWARD_ROW_CACHE_SIZE } {
  }
  ForwardRowData(const nebula::type::Schema& input,
                 const TransformerVector& transformers,
                 std::unique_ptr<RowData> inner)
    : SchemaRow(input),
      inner_{ std::move(inner) },
      row_{ inner_.get() },
      transformers_{ transformers },
      values_{ FLAGS_FOWARD_ROW_CACHE_SIZE } {
  }

public:
  inline bool isNull(IndexType i) const override {
    return row_->isNull(i);
  }

  // TODO(cao): only support int128 for now
  // TOOD(cao): flatrow supports string key only, should support int key too
#define DISPATCH_TRANSFORM_TYPE(T, F)      \
  T F(IndexType i) const override {        \
    auto transform = transformers_.at(i);  \
    if (transform) {                       \
      auto key = std::to_string(i);        \
      if (values_.hasKey(key)) {           \
        LOG(INFO) << "cache hit: " << key; \
        return values_.F(key);             \
      }                                    \
      T v;                                 \
      transform(row_, &v);                 \
      values_.write(key, v);               \
      return v;                            \
    }                                      \
    return row_->F(i);                     \
  }

  DISPATCH_TRANSFORM_TYPE(bool, readBool)
  DISPATCH_TRANSFORM_TYPE(int8_t, readByte)
  DISPATCH_TRANSFORM_TYPE(int16_t, readShort)
  DISPATCH_TRANSFORM_TYPE(int32_t, readInt)
  DISPATCH_TRANSFORM_TYPE(int64_t, readLong)
  DISPATCH_TRANSFORM_TYPE(float, readFloat)
  DISPATCH_TRANSFORM_TYPE(double, readDouble)

#undef DISPATCH_TRANSFORM_TYPE

  int128_t readInt128(IndexType i) const override {
    return row_->readInt128(i);
  }

  std::string_view readString(IndexType i) const override {
    return row_->readString(i);
  }

  // compound types
  std::unique_ptr<nebula::surface::ListData> readList(IndexType) const override {
    throw NException("Not implemented yet");
  }
  std::unique_ptr<nebula::surface::MapData> readMap(IndexType) const override {
    throw NException("Not implemented yet");
  }

public:
  inline void set(const RowData& row) {
    row_ = const_cast<RowData*>(&row);
    values_.reset();
  }

private:
  std::unique_ptr<RowData> inner_;
  RowData* row_;
  const TransformerVector& transformers_;

  // value cache so it don't call into transformer repeatedly
  mutable FlatRow values_;
};

class ForwardRowCursor : public RowCursor {
public:
  ForwardRowCursor(RowCursorPtr inner, const FinalPhase& phase)
    : RowCursor(inner->size()),
      inner_{ inner },
      phase_{ phase },
      row_{ phase.inputSchema(), transformers_ } {
    // build transformers
    buildTransformers();
  }
  virtual ~ForwardRowCursor() = default;

  virtual const RowData& next() override {
    row_.set(inner_->next());
    return row_;
  }

  // a const interface return an unique ptr for secure randome access
  virtual std::unique_ptr<RowData> item(size_t i) const override {
    return std::unique_ptr<RowData>(
      new ForwardRowData(phase_.inputSchema(), transformers_, inner_->item(i)));
  }

private:
  void buildTransformers() {
    auto input = phase_.inputSchema();
    auto output = phase_.outputSchema();
    // different on this column index
    const auto size = input->size();
    N_ENSURE_EQ(size, output->size(), "support the same number of columns");
    transformers_.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      auto iType = input->childType(i)->k();
      auto oType = output->childType(i)->k();
      transformers_.push_back({});

      if (phase_.isAggregateColumn(i)) {
// below provides a template to write core logic for all input / output type combinations
#define LOGIC_BY_IO(O, I)                                                        \
  case Kind::I: {                                                                \
    transformers_[i] = [i](const RowData* r, void* t) {                          \
      using OutputType = TypeTraits<Kind::O>::CppType;                           \
      auto sketch = r->getAggregator(i);                                         \
      N_ENSURE_NOT_NULL(sketch, "transformer only built on sketch type");        \
      auto agg = std::static_pointer_cast<Aggregator<Kind::O, Kind::I>>(sketch); \
      *static_cast<OutputType*>(t) = agg->finalize();                            \
    };                                                                           \
    break;                                                                       \
  }

        ITERATE_BY_IO(oType, iType)

#undef LOGIC_BY_IO
      }
    }
  }

private:
  RowCursorPtr inner_;
  const FinalPhase& phase_;
  ForwardRowData row_;
  TransformerVector transformers_;
};

// finalize transform data between types if needed, otherwise you get the original cursor
nebula::surface::RowCursorPtr finalize(nebula::surface::RowCursorPtr cursor, const FinalPhase& phase) {
  if (!phase.hasAggregation()) {
    return cursor;
  }

  // now let's return a cursor that has transformer built in
  return std::make_shared<ForwardRowCursor>(cursor, phase);
}

} // namespace core
} // namespace execution
} // namespace nebula