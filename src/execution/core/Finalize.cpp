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
using nebula::surface::eval::UDAF;
using nebula::type::Kind;
using nebula::type::Schema;

// transformers to be executed for each column if it needs a transformer
using TransformerVector = std::vector<std::function<void(const int128_t& v, void* t)>>;

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
      auto i128 = row_->readInt128(i);     \
      T v;                                 \
      transform(i128, &v);                 \
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
  ForwardRowCursor(RowCursorPtr inner,
                   Schema input,
                   Schema output,
                   const nebula::surface::eval::Fields& fields)
    : RowCursor(inner->size()),
      inner_{ inner },
      input_{ input },
      output_{ output },
      fields_{ fields },
      row_{ input, transformers_ } {
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
    return std::unique_ptr<RowData>(new ForwardRowData(input_, transformers_, inner_->item(i)));
  }

private:
  void buildTransformers() {
    // different on this column index
    const auto size = input_->size();
    N_ENSURE_EQ(size, output_->size(), "support the same number of columns");
    transformers_.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      auto iType = input_->childType(i)->k();
      auto oType = output_->childType(i)->k();
      auto different = iType != oType;
      transformers_.push_back({});

      if (different) {
        N_ENSURE_EQ(iType, Kind::INT128, "support transform from int128 to other types only");
        // TODO(cao): ideally this should be a entry point for ValueEval - the base for all expressions
        // However, so far I don't see any cases more than UDAF could have this, so jump directly to it for now
#define DISPATCH_TYPE_FROM_I128(K)                                                                                        \
  case Kind::K: {                                                                                                         \
    transformers_[i] = [& udaf = static_cast<UDAF<Kind::K, Kind::INT128>&>(*fields_.at(i))](const int128_t& v, void* t) { \
      *static_cast<nebula::type::TypeTraits<Kind::K>::CppType*>(t) = udaf.finalize(v);                                    \
    };                                                                                                                    \
    break;                                                                                                                \
  }

        switch (oType) {
          DISPATCH_TYPE_FROM_I128(TINYINT)
          DISPATCH_TYPE_FROM_I128(SMALLINT)
          DISPATCH_TYPE_FROM_I128(INTEGER)
          DISPATCH_TYPE_FROM_I128(BIGINT)
          DISPATCH_TYPE_FROM_I128(REAL)
          DISPATCH_TYPE_FROM_I128(DOUBLE)
        default:
          throw NException(fmt::format("type {0} not supported.", nebula::type::TypeBase::kname(oType)));
        }
#undef DISPATCH_TYPE_FROM_I128
      }
    }
  }

private:
  RowCursorPtr inner_;
  Schema input_;
  Schema output_;
  const nebula::surface::eval::Fields& fields_;
  ForwardRowData row_;
  TransformerVector transformers_;
};

// finalize transform data between types if needed, otherwise you get the original cursor
nebula::surface::RowCursorPtr finalize(nebula::surface::RowCursorPtr cursor, const FinalPhase& phase) {
  if (!phase.diffInputOutput()) {
    return cursor;
  }

  // now let's return a cursor that has transformer built in
  return std::make_shared<ForwardRowCursor>(cursor, phase.inputSchema(), phase.outputSchema(), phase.fields());
}

} // namespace core
} // namespace execution
} // namespace nebula