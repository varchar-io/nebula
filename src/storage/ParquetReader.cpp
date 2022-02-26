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

#include "ParquetReader.h"

#include "common/Conv.h"
#include "common/Likely.h"

/**
 * Parquet reader to read a local parquet file and produce Nebula Rows
 */
namespace nebula {
namespace storage {

using nebula::memory::FlatRow;
using nebula::surface::RowData;
using nebula::type::Kind;

// write value as target type defined
// since user schema may be different from file schema.
template <class T>
void writeAsKind(Kind kind, FlatRow& row, const std::string& name, const T& value) {
#define KIND_CONVERT(K)                                                          \
  case Kind::K: {                                                                \
    using TargetType = nebula::type::TypeTraits<nebula::type::Kind::K>::CppType; \
    row.write(name, nebula::common::safe_to<TargetType>(value));                 \
    break;                                                                       \
  }

  switch (kind) {
    KIND_CONVERT(BOOLEAN)
    KIND_CONVERT(TINYINT)
    KIND_CONVERT(SMALLINT)
    KIND_CONVERT(INTEGER)
    KIND_CONVERT(BIGINT)
    KIND_CONVERT(REAL)
    KIND_CONVERT(DOUBLE)
  default:
    // other types not supported - write NULL instead
    row.writeNull(name);
  }

#undef KIND_CONVERT
}

const RowData& ParquetReader::next() {
  // TODO build a flat row out of a reader
  if (N_UNLIKELY(groupReader_ == nullptr) || cursorInGroup_ == groupRows_) {
    groupReader_ = reader_->RowGroup(group_++);
    cursorInGroup_ = 0;
    groupRows_ = groupReader_->metadata()->num_rows();

    // initialize all column readers in the meta store
    for (auto& item : this->columns_) {
      item.second.reader = groupReader_->Column(item.second.columnIndex);
    }
  }

  // read current data from current group reader and set it to current FlatRow
  row_.reset();
#define TRANSFER_FROM_PARQUET(T, K, R)                         \
  case Kind::K: {                                              \
    auto reader = static_cast<parquet::R*>(info.reader.get()); \
    T value;                                                   \
    N_ENSURE(reader->HasNext(), "should have next");           \
    reader->ReadBatch(1, &defLevel, nullptr, &value, &vread);  \
    if (vread == 0) {                                          \
      row_.writeNull(name);                                    \
    } else {                                                   \
      if (info.kind == Kind::K) {                              \
        row_.write(name, value);                               \
      } else {                                                 \
        writeAsKind(info.kind, row_, name, value);             \
      }                                                        \
    }                                                          \
    break;                                                     \
  }

  for (auto itr = this->columns_.begin(), end = this->columns_.end(); itr != end; ++itr) {
    auto name = itr->first;
    auto info = itr->second;
    // number of non-nulls read
    int64_t vread = 0;

    // definition level - required in reader to able to skip nulls
    int16_t defLevel;

    // bool, int, long, float, double, string
    switch (info.realKind) {
      TRANSFER_FROM_PARQUET(bool, BOOLEAN, BoolReader)
      // a bit obsecure: use int32 reader and data type to read SMALL int
      TRANSFER_FROM_PARQUET(int32_t, TINYINT, Int32Reader)
      TRANSFER_FROM_PARQUET(int32_t, SMALLINT, Int32Reader)
      TRANSFER_FROM_PARQUET(int32_t, INTEGER, Int32Reader)
      TRANSFER_FROM_PARQUET(int64_t, BIGINT, Int64Reader)
      TRANSFER_FROM_PARQUET(float, REAL, FloatReader)
      TRANSFER_FROM_PARQUET(double, DOUBLE, DoubleReader)
    case Kind::VARCHAR: {
      auto reader = static_cast<parquet::ByteArrayReader*>(info.reader.get());
      parquet::ByteArray value;
      reader->ReadBatch(1, &defLevel, nullptr, &value, &vread);
      if (vread == 0) {
        row_.writeNull(name);
      } else {
        std::string str((const char*)value.ptr, (size_t)value.len);
        if (info.kind == Kind::VARCHAR) {
          row_.write(name, str);
        } else {
          writeAsKind(info.kind, row_, name, str);
        }
      }
      break;
    }
    default:
      throw NException("Type not supported yet");
    }
  }

#undef TRANSFER_FROM_PARQUET

  // row is ready to consume
  cursorInGroup_++;
  index_++;
  return row_;
}

} // namespace storage
} // namespace nebula