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

#define PTYPE_CONV_CASE_VALIDATION(PT, NT)                                                               \
  case parquet::Type::type::PT: {                                                                        \
    this->columns_[cname] = { i, kind, nullptr };                                                        \
    auto typeSafetyCheck = nebula::type::ConvertibleFrom<nebula::type::Kind::NT>::convertibleFrom(kind); \
    if (!typeSafetyCheck) {                                                                              \
      throw NException(fmt::format("Type mismatch from {0} to {1}.", kind, nebula::type::Kind::NT));     \
    }                                                                                                    \
    break;                                                                                               \
  }

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
      schema_->onChild(cname, [&kind](const nebula::type::TypeNode& node) {
        // found a node
        kind = node->k();
      });

      // if this column is in requested schema
      if (kind != nebula::type::Kind::INVALID) {
        found++;
        // ensure the type is supported
        switch (lt) {
          PTYPE_CONV_CASE_VALIDATION(BOOLEAN, BOOLEAN)
          PTYPE_CONV_CASE_VALIDATION(INT32, INTEGER)
          PTYPE_CONV_CASE_VALIDATION(INT64, BIGINT)
          PTYPE_CONV_CASE_VALIDATION(FLOAT, REAL)
          PTYPE_CONV_CASE_VALIDATION(DOUBLE, DOUBLE)
          PTYPE_CONV_CASE_VALIDATION(BYTE_ARRAY, VARCHAR)
        default: throw NException("unsupported type");
        }
      }
    }

    // every node should be validated
    N_ENSURE(found == schema_->size(), "every node in desired schema should be present");

#undef PTYPE_CONV_CASE_VALIDATION
  }

  ParquetReader(const std::string& file)
    : ParquetReader(file, nullptr) {
// build up schema with all columns
// TODO(cao) - use tree visitor to convert schema tree
// right now, here we only support single plain schema struct[primitive,...]
// with limited types of Parquet::Type as
// BOOLEAN = 0,
// INT32 = 1,
// INT64 = 2,
// FLOAT = 4,
// DOUBLE = 5,
// BYTE_ARRAY = 6
#define PTYPE_CONV_CASE(PT, NT)                                     \
  case parquet::Type::type::PT: {                                   \
    this->columns_[cname] = { i, nebula::type::NT::kind, nullptr }; \
    nodes.push_back(nebula::type::NT::createTree(cd->name()));      \
    break;                                                          \
  }

    const auto* schema = this->meta_->schema();
    std::vector<nebula::type::TreeNode> nodes;
    for (size_t i = 0, size = schema->num_columns(); i < size; ++i) {
      // column description
      auto cd = schema->Column(i);
      std::string cname(cd->name());
      const auto lt = cd->physical_type();

      switch (lt) {
        PTYPE_CONV_CASE(BOOLEAN, BoolType)
        PTYPE_CONV_CASE(INT32, IntType)
        PTYPE_CONV_CASE(INT64, LongType)
        PTYPE_CONV_CASE(FLOAT, FloatType)
        PTYPE_CONV_CASE(DOUBLE, DoubleType)
        PTYPE_CONV_CASE(BYTE_ARRAY, StringType)
      default:
        break;
      }
    }

#undef PTYPE_CONV_CASE
    // row name indicating this schema is from parquet
    this->schema_ = std::static_pointer_cast<nebula::type::RowType>(
      nebula::type::RowType::create("parquet", nodes));
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