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

#include <folly/Conv.h>
#include <folly/String.h>
#include <fstream>
#include <iostream>
#include <parquet/api/reader.h>
#include <string>

#include "common/Errors.h"
#include "memory/FlatRow.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

namespace {
  bool validateTypeConversion(parquet::Type::type parquetType, nebula::type::Kind nebulaType) {
    switch (parquetType) {
      case parquet::Type::type::BOOLEAN:
        return nebula::type::ConvertibleFrom<nebula::type::Kind::BOOLEAN>::convertibleFrom(nebulaType);
      case parquet::Type::type::INT32:
        return nebula::type::ConvertibleFrom<nebula::type::Kind::INTEGER>::convertibleFrom(nebulaType);
      case parquet::Type::type::INT64:
        return nebula::type::ConvertibleFrom<nebula::type::Kind::BIGINT>::convertibleFrom(nebulaType);
      case parquet::Type::type::FLOAT:
        return nebula::type::ConvertibleFrom<nebula::type::Kind::REAL>::convertibleFrom(nebulaType);
      case parquet::Type::type::DOUBLE:
        return nebula::type::ConvertibleFrom<nebula::type::Kind::DOUBLE>::convertibleFrom(nebulaType);
      case parquet::Type::type::BYTE_ARRAY:
        return (
          nebula::type::ConvertibleFrom<nebula::type::Kind::VARCHAR>::convertibleFrom(nebulaType)
          || nebula::type::ConvertibleFrom<nebula::type::Kind::DOUBLE>::convertibleFrom(nebulaType)
        );
      default:
        throw NException(fmt::format("unsupported type parquet type {0}", parquetType));
    }
    // will never reach this
    return false;
  }
}

/**
 * Parquet reader to read a local parquet file and produce Nebula Rows
 */
namespace nebula {
namespace storage {

// column info:
//  column index in parquet schema,
//  nebula type for this column,
//  column reader for this column in given group context
struct ColumnInfo {
  size_t columnIndex;
  nebula::type::Kind kind;
  std::shared_ptr<parquet::ColumnReader> reader;
};

// create a parquet reader to provide nebula rows
// open file without memory mapping - possible optimize for smaller file.
// passed-in schema specified columns needed using name matching
class ParquetReader : public nebula::surface::RowCursor {
  static constexpr size_t SLICE_SIZE = 1024;

public:
  ParquetReader(const std::string& file, nebula::type::Schema schema)
    : nebula::surface::RowCursor(0),
      reader_{ parquet::ParquetFileReader::OpenFile(file, false) },
      group_{ 0 },
      schema_{ schema },
      row_{ SLICE_SIZE } {
    // fetch schema from the given file
    N_ENSURE_NOT_NULL(reader_, "valid parquet reader is required");
    this->meta_ = reader_->metadata();
    // set total rows as
    size_ = this->meta_->num_rows();

    // TODO(cao) - note that, schema is a tree we need comprehensive conversion
    // But here, we only support plain schema.
    // build up name to index mapping with validation
    const auto* ps = this->meta_->schema();
    size_t found = 0;
    for (size_t i = 0, size = ps->num_columns(); i < size; ++i) {
      auto cd = ps->Column(i);
      std::string cname(cd->name());
      const auto lt = cd->physical_type();

      auto kind = nebula::type::Kind::INVALID;
      schema_->onChild(cname, [&kind, &cname](const nebula::type::TypeNode& node) {
        // found a node
        kind = node->k();
      });
      // if this column is in requested schema
      if (kind != nebula::type::Kind::INVALID) {
        // ensure the type is supported
        if (!validateTypeConversion(lt, kind)) {
          throw NException(fmt::format("unsupported type conversion, physical type {0} to nebula type {1}", lt, kind));
        }
        this->columns_[cname] = { i, kind, nullptr };
        found++;
      }
    }

    // every node should be validated
    N_ENSURE(found == schema_->size(), "every node in desired schema should be present");
  }

  virtual ~ParquetReader() = default;

  // next row data of CsvRow
  virtual const nebula::surface::RowData& next() override;

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t) const override {
    throw NException("Parquet Reader does not support random access by row number");
  }

private:
  std::unique_ptr<parquet::ParquetFileReader> reader_;
  size_t group_;
  nebula::type::Schema schema_;

  std::shared_ptr<parquet::FileMetaData> meta_;
  nebula::common::unordered_map<std::string, ColumnInfo> columns_;

  // current group reader and total rows in this group
  std::shared_ptr<parquet::RowGroupReader> groupReader_;
  size_t cursorInGroup_;
  size_t groupRows_;

  // the row to be visited
  nebula::memory::FlatRow row_;
}; // namespace storage
} // namespace storage
} // namespace nebula