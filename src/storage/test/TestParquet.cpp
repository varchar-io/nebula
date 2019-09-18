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

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <fmt/format.h>
#include <folly/Conv.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/types.h>

#include "common/Evidence.h"
#include "storage/ParquetReader.h"
#include "storage/aws/S3.h"
#include "storage/local/File.h"
#include "type/Serde.h"

namespace nebula {
namespace storage {
namespace test {

using ConvertedType = parquet::LogicalType::type;
using nebula::surface::RowData;
using nebula::type::TypeSerializer;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

constexpr int FIXED_LENGTH = 10;
// this is for testing only since it's small, RG size is better to be 32M +
constexpr int64_t ROW_GROUP_SIZE = 8 * 1024 * 1024;

static std::shared_ptr<GroupNode> SetupSchema() {
  parquet::schema::NodeVector fields;
  // Create a primitive node named 'boolean_field' with type:BOOLEAN,
  // repetition:REQUIRED
  fields.push_back(PrimitiveNode::Make("boolean_field", Repetition::REQUIRED,
                                       Type::BOOLEAN, ConvertedType::NONE));

  // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED,
  // logical type:TIME_MILLIS
  fields.push_back(PrimitiveNode::Make("int32_field", Repetition::REQUIRED, Type::INT32,
                                       ConvertedType::TIME_MILLIS));

  // Create a primitive node named 'int64_field' with type:INT64, repetition:REPEATED
  fields.push_back(PrimitiveNode::Make("int64_field", Repetition::REPEATED, Type::INT64,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make("int96_field", Repetition::REQUIRED, Type::INT96,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make("float_field", Repetition::REQUIRED, Type::FLOAT,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make("double_field", Repetition::REQUIRED, Type::DOUBLE,
                                       ConvertedType::NONE));

  // Create a primitive node named 'ba_field' with type:BYTE_ARRAY, repetition:OPTIONAL
  fields.push_back(PrimitiveNode::Make("ba_field", Repetition::OPTIONAL, Type::BYTE_ARRAY,
                                       ConvertedType::NONE));

  // Create a primitive node named 'flba_field' with type:FIXED_LEN_BYTE_ARRAY,
  // repetition:REQUIRED, field_length = FIXED_LENGTH
  fields.push_back(PrimitiveNode::Make("flba_field", Repetition::REQUIRED,
                                       Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE,
                                       FIXED_LENGTH));

  // Create a GroupNode named 'schema' using the primitive nodes defined above
  // This GroupNode is the root node of the schema tree
  return std::static_pointer_cast<GroupNode>(
    GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

bool writeParquetFile(const std::string& file, size_t rows) {
  try {
    // Create a local file output stream instance.
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    PARQUET_THROW_NOT_OK(FileClass::Open(file, &out_file));

    // Setup the parquet schema
    std::shared_ptr<GroupNode> schema = SetupSchema();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer = parquet::ParquetFileWriter::Open(out_file, schema, props);

    // Append a BufferedRowGroup to keep the RowGroup open until a certain size
    parquet::RowGroupWriter* rg_writer = file_writer->AppendBufferedRowGroup();

    int num_columns = file_writer->num_columns();
    std::vector<int64_t> buffered_values_estimate(num_columns, 0);
    for (size_t i = 0; i < rows; i++) {
      int64_t estimated_bytes = 0;
      // Get the estimated size of the values that are not written to a page yet
      for (int n = 0; n < num_columns; n++) {
        estimated_bytes += buffered_values_estimate[n];
      }

      // We need to consider the compressed pages
      // as well as the values that are not compressed yet
      if ((rg_writer->total_bytes_written() + rg_writer->total_compressed_bytes() + estimated_bytes) > ROW_GROUP_SIZE) {
        rg_writer->Close();
        std::fill(buffered_values_estimate.begin(), buffered_values_estimate.end(), 0);
        rg_writer = file_writer->AppendBufferedRowGroup();
      }

      int col_id = 0;
      // Write the Bool column
      parquet::BoolWriter* bool_writer = static_cast<parquet::BoolWriter*>(rg_writer->column(col_id));
      bool bool_value = ((i % 2) == 0) ? true : false;
      bool_writer->WriteBatch(1, nullptr, nullptr, &bool_value);
      buffered_values_estimate[col_id] = bool_writer->EstimatedBufferedValueBytes();

      // Write the Int32 column
      col_id++;
      parquet::Int32Writer* int32_writer = static_cast<parquet::Int32Writer*>(rg_writer->column(col_id));
      int32_t int32_value = i;
      int32_writer->WriteBatch(1, nullptr, nullptr, &int32_value);
      buffered_values_estimate[col_id] = int32_writer->EstimatedBufferedValueBytes();

      // Write the Int64 column. Each row has repeats twice.
      col_id++;
      parquet::Int64Writer* int64_writer = static_cast<parquet::Int64Writer*>(rg_writer->column(col_id));
      int64_t int64_value1 = 2 * i;
      int16_t definition_level = 1;
      int16_t repetition_level = 0;
      int64_writer->WriteBatch(1, &definition_level, &repetition_level, &int64_value1);
      int64_t int64_value2 = (2 * i + 1);
      repetition_level = 1; // start of a new record
      int64_writer->WriteBatch(1, &definition_level, &repetition_level, &int64_value2);
      buffered_values_estimate[col_id] = int64_writer->EstimatedBufferedValueBytes();

      // Write the INT96 column.
      col_id++;
      parquet::Int96Writer* int96_writer = static_cast<parquet::Int96Writer*>(rg_writer->column(col_id));
      parquet::Int96 int96_value;
      int96_value.value[0] = i;
      int96_value.value[1] = i + 1;
      int96_value.value[2] = i + 2;
      int96_writer->WriteBatch(1, nullptr, nullptr, &int96_value);
      buffered_values_estimate[col_id] = int96_writer->EstimatedBufferedValueBytes();

      // Write the Float column
      col_id++;
      parquet::FloatWriter* float_writer = static_cast<parquet::FloatWriter*>(rg_writer->column(col_id));
      float float_value = static_cast<float>(i) * 1.1f;
      float_writer->WriteBatch(1, nullptr, nullptr, &float_value);
      buffered_values_estimate[col_id] = float_writer->EstimatedBufferedValueBytes();

      // Write the Double column
      col_id++;
      parquet::DoubleWriter* double_writer = static_cast<parquet::DoubleWriter*>(rg_writer->column(col_id));
      double double_value = i * 1.1111111;
      double_writer->WriteBatch(1, nullptr, nullptr, &double_value);
      buffered_values_estimate[col_id] = double_writer->EstimatedBufferedValueBytes();

      // Write the ByteArray column. Make every alternate values NULL
      col_id++;
      parquet::ByteArrayWriter* ba_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(col_id));
      parquet::ByteArray ba_value;
      char hello[FIXED_LENGTH] = "parquet";
      hello[7] = static_cast<char>(static_cast<int>('0') + i / 100);
      hello[8] = static_cast<char>(static_cast<int>('0') + (i / 10) % 10);
      hello[9] = static_cast<char>(static_cast<int>('0') + i % 10);
      if (i % 2 == 0) {
        int16_t definition_level = 1;
        ba_value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
        ba_value.len = FIXED_LENGTH;
        ba_writer->WriteBatch(1, &definition_level, nullptr, &ba_value);
      } else {
        int16_t definition_level = 0;
        ba_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
      }
      buffered_values_estimate[col_id] = ba_writer->EstimatedBufferedValueBytes();

      // Write the FixedLengthByteArray column
      col_id++;
      parquet::FixedLenByteArrayWriter* flba_writer = static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->column(col_id));
      parquet::FixedLenByteArray flba_value;
      char v = static_cast<char>(i);
      char flba[FIXED_LENGTH] = { v, v, v, v, v, v, v, v, v, v };
      flba_value.ptr = reinterpret_cast<const uint8_t*>(&flba[0]);

      flba_writer->WriteBatch(1, nullptr, nullptr, &flba_value);
      buffered_values_estimate[col_id] = flba_writer->EstimatedBufferedValueBytes();
    }

    // Close the RowGroupWriter
    rg_writer->Close();
    // Close the ParquetFileWriter
    file_writer->Close();

    // Write the bytes to file
    DCHECK(out_file->Close().ok());
    return true;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Parquet write error: " << e.what();
    return false;
  }
}

// Parquet file reader and writer are included in arrow
// Test here is to demonstrate API availability
// This is copied from below address for internal integration purpose
// https://github.com/apache/arrow/blob/master/cpp/examples/parquet/low-level-api/reader-writer2.cc
// use this example for arrow format adatped read/write api
TEST(ParquetTest, TestParquetReadWrite) {
  /**********************************************************************************
                             PARQUET WRITER EXAMPLE
  **********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  nebula::common::Evidence::Duration duration;
  const char readWriteSample[] = "parquet_cpp_example2.parquet";
  constexpr auto numRows = 25000;
  writeParquetFile(readWriteSample, numRows);
  LOG(INFO) << fmt::format("Writing {0} rows taking {1} ms", numRows, duration.elapsedMs());
  duration.reset();

  /**********************************************************************************
                             PARQUET READER EXAMPLE
  **********************************************************************************/

  try {
    // Create a ParquetReader instance
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(readWriteSample, false);

    // Get the File MetaData
    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

    int num_row_groups = file_metadata->num_row_groups();

    // Get the number of Columns
    int num_columns = file_metadata->num_columns();
    assert(num_columns == 8);

    std::vector<int> col_row_counts(num_columns, 0);

    // Iterate over all the RowGroups in the file
    for (int r = 0; r < num_row_groups; ++r) {
      // Get the RowGroup Reader
      std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);

      assert(row_group_reader->metadata()->total_byte_size() < ROW_GROUP_SIZE);

      int64_t values_read = 0;
      int64_t rows_read = 0;
      int16_t definition_level;
      int16_t repetition_level;
      std::shared_ptr<parquet::ColumnReader> column_reader;
      int col_id = 0;

      // Get the Column Reader for the boolean column
      column_reader = row_group_reader->Column(col_id);
      parquet::BoolReader* bool_reader = static_cast<parquet::BoolReader*>(column_reader.get());

      // Read all the rows in the column
      while (bool_reader->HasNext()) {
        bool value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = bool_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        bool expected_value = ((col_row_counts[col_id] % 2) == 0) ? true : false;
        assert(value == expected_value);
        col_row_counts[col_id]++;
      }

      // Get the Column Reader for the Int32 column
      col_id++;
      column_reader = row_group_reader->Column(col_id);
      parquet::Int32Reader* int32_reader = static_cast<parquet::Int32Reader*>(column_reader.get());
      // Read all the rows in the column
      while (int32_reader->HasNext()) {
        int32_t value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = int32_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        assert(value == col_row_counts[col_id]);
        col_row_counts[col_id]++;
      }

      // Get the Column Reader for the Int64 column
      col_id++;
      column_reader = row_group_reader->Column(col_id);
      parquet::Int64Reader* int64_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
      // Read all the rows in the column
      while (int64_reader->HasNext()) {
        int64_t value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = int64_reader->ReadBatch(1, &definition_level, &repetition_level,
                                            &value, &values_read);
        // Ensure only one value is read
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        int64_t expected_value = col_row_counts[col_id];
        assert(value == expected_value);
        if ((col_row_counts[col_id] % 2) == 0) {
          assert(repetition_level == 0);
        } else {
          assert(repetition_level == 1);
        }
        col_row_counts[col_id]++;
      }

      // Get the Column Reader for the Int96 column
      col_id++;
      column_reader = row_group_reader->Column(col_id);
      parquet::Int96Reader* int96_reader = static_cast<parquet::Int96Reader*>(column_reader.get());
      // Read all the rows in the column
      while (int96_reader->HasNext()) {
        parquet::Int96 value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = int96_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        parquet::Int96 expected_value;
        expected_value.value[0] = col_row_counts[col_id];
        expected_value.value[1] = col_row_counts[col_id] + 1;
        expected_value.value[2] = col_row_counts[col_id] + 2;
        for (int j = 0; j < 3; j++) {
          assert(value.value[j] == expected_value.value[j]);
        }
        col_row_counts[col_id]++;
      }

      // Get the Column Reader for the Float column
      col_id++;
      column_reader = row_group_reader->Column(col_id);
      parquet::FloatReader* float_reader = static_cast<parquet::FloatReader*>(column_reader.get());
      // Read all the rows in the column
      while (float_reader->HasNext()) {
        float value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = float_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        float expected_value = static_cast<float>(col_row_counts[col_id]) * 1.1f;
        assert(value == expected_value);
        col_row_counts[col_id]++;
      }

      // Get the Column Reader for the Double column
      col_id++;
      column_reader = row_group_reader->Column(col_id);
      parquet::DoubleReader* double_reader = static_cast<parquet::DoubleReader*>(column_reader.get());
      // Read all the rows in the column
      while (double_reader->HasNext()) {
        double value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = double_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        double expected_value = col_row_counts[col_id] * 1.1111111;
        assert(value == expected_value);
        col_row_counts[col_id]++;
      }

      // Get the Column Reader for the ByteArray column
      col_id++;
      column_reader = row_group_reader->Column(col_id);
      parquet::ByteArrayReader* ba_reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());
      // Read all the rows in the column
      while (ba_reader->HasNext()) {
        parquet::ByteArray value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = ba_reader->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
        // Ensure only one value is read
        assert(rows_read == 1);
        // Verify the value written
        char expected_value[FIXED_LENGTH] = "parquet";
        expected_value[7] = static_cast<char>('0' + col_row_counts[col_id] / 100);
        expected_value[8] = static_cast<char>('0' + (col_row_counts[col_id] / 10) % 10);
        expected_value[9] = static_cast<char>('0' + col_row_counts[col_id] % 10);
        if (col_row_counts[col_id] % 2 == 0) { // only alternate values exist
          // There are no NULL values in the rows written
          assert(values_read == 1);
          assert(value.len == FIXED_LENGTH);
          assert(memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0);
          assert(definition_level == 1);
        } else {
          // There are NULL values in the rows written
          assert(values_read == 0);
          assert(definition_level == 0);
        }
        col_row_counts[col_id]++;
      }

      // Get the Column Reader for the FixedLengthByteArray column
      col_id++;
      column_reader = row_group_reader->Column(col_id);
      parquet::FixedLenByteArrayReader* flba_reader = static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());
      // Read all the rows in the column
      while (flba_reader->HasNext()) {
        parquet::FixedLenByteArray value;
        // Read one value at a time. The number of rows read is returned. values_read
        // contains the number of non-null rows
        rows_read = flba_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
        // Ensure only one value is read
        assert(rows_read == 1);
        // There are no NULL values in the rows written
        assert(values_read == 1);
        // Verify the value written
        char v = static_cast<char>(col_row_counts[col_id]);
        char expected_value[FIXED_LENGTH] = { v, v, v, v, v, v, v, v, v, v };
        assert(memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0);
        col_row_counts[col_id]++;
      }
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "Parquet read error: " << e.what();
    return;
  }

  LOG(INFO) << fmt::format("Reading {0} rows and verify taking {1} ms",
                           numRows, duration.elapsedMs());
}

TEST(ParquetTest, TestReadThroughFlatRow) {
  const char readWriteSample[] = "parquet_sample_file_read_through_flatrow";
  constexpr auto numRows = 259999;
  EXPECT_TRUE(writeParquetFile(readWriteSample, numRows));

  // read this file through ParquetReader
  auto schema = TypeSerializer::from(
    "ROW<boolean_field:bool, int32_field:int, int64_field:long,"
    "float_field:float, double_field:double, ba_field:string>");

  // read some of the columns from the parquet file
  auto toString = [](const RowData& row) -> std::string {
    return fmt::format(
      "b={0}, i={1}, l={2}, f={3}, d={4}, s={5}",
      row.isNull("boolean_field") ? "NULL" : folly::to<std::string>(row.readBool("boolean_field")),
      row.isNull("int32_field") ? "NULL" : folly::to<std::string>(row.readInt("int32_field")),
      row.isNull("int64_field") ? "NULL" : folly::to<std::string>(row.readLong("int64_field")),
      row.isNull("float_field") ? "NULL" : folly::to<std::string>(row.readFloat("float_field")),
      row.isNull("double_field") ? "NULL" : folly::to<std::string>(row.readDouble("double_field")),
      row.isNull("ba_field") ? "NULL" : row.readString("ba_field"));
  };
  ParquetReader reader(readWriteSample, schema);
  auto rows = 0;
  while (reader.hasNext()) {
    auto& line = reader.next();
    rows++;
    if (rows > numRows - 10) {
      LOG(INFO) << toString(line);
    }
  }

  EXPECT_EQ(rows, numRows);
}

TEST(ParquetTest, DISABLED_TestRealParquetFile) {
  auto localFile = "/tmp/parquet.f";
  auto schema = TypeSerializer::from("ROW<id:long, user_id:long, link_domain:string, title:string, details:string, image_signature:string>");
  ParquetReader reader(localFile, schema);
  auto rows = 0;
  auto toString = [](const RowData& row) -> std::string {
    return fmt::format(
      "b={0}, i={1}, l={2}, f={3}, d={4}, s={5}",
      row.isNull("id") ? "NULL" : folly::to<std::string>(row.readLong("id")),
      row.isNull("user_id") ? "NULL" : folly::to<std::string>(row.readLong("user_id")),
      row.isNull("link_domain") ? "NULL" : row.readString("link_domain"),
      row.isNull("title") ? "NULL" : row.readString("title"),
      row.isNull("details") ? "NULL" : row.readString("details"),
      row.isNull("image_signature") ? "NULL" : row.readString("image_signature"));
  };

  LOG(INFO) << "Total rows: " << reader.size();
  while (reader.hasNext()) {
    const auto& r = reader.next();
    LOG(INFO) << "ROW" << rows++ << ": " << toString(r);
  }

  EXPECT_EQ(rows, reader.size());
}

TEST(ParquetTest, DISABLED_TestEventsParquet) {
  auto localFile = "/tmp/events.p";
  auto schema = TypeSerializer::from("ROW<dt:string, eventtype:string, image_signature:string, country:string>");
  ParquetReader reader(localFile, schema);
  while (reader.hasNext()) {
    const auto& r = reader.next();
    LOG(INFO) << (r.isNull("image_signature") ? "NULL" : r.readString("image_signature"));
  }

  LOG(INFO) << "Total rows: " << reader.size();
}

} // namespace test
} // namespace storage
} // namespace nebula