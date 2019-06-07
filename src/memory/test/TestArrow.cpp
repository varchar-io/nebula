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

#include <arrow/api.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <valarray>
#include "arrow/io/api.h"
#include "common/Memory.h"
#include "fmt/format.h"
#include "memory/Batch.h"
#include "memory/DataNode.h"
#include "memory/FlatRow.h"
#include "meta/TestTable.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "surface/StaticData.h"
#include "type/Serde.h"

namespace nebula {
namespace memory {
namespace test {

using arrow::DoubleBuilder;
using arrow::Int64Builder;
using arrow::ListBuilder;
using nebula::common::Evidence;
using nebula::surface::IndexType;
using nebula::surface::MockRowData;
using nebula::type::ROOT;
using nebula::type::TypeSerializer;

// the example is copied from git repo
// https://github.com/apache/arrow/blob/master/cpp/examples/arrow/row-wise-conversion-example.cc
struct data_row {
  int64_t id;
  double cost;
  std::vector<double> cost_components;
};

// Transforming a vector of structs into a columnar Table.
//
// The final representation should be an `arrow::Table` which in turn is made up of
// an `arrow::Schema` and a list of `arrow::Column`. An `arrow::Column` is again a
// named collection of one or more `arrow::Array` instances. As the first step, we
// will iterate over the data and build up the arrays incrementally. For this task,
// we provide `arrow::ArrayBuilder` classes that help in the construction of the
// final `arrow::Array` instances.
//
// For each type, Arrow has a specially typed builder class. For the primitive
// values `id` and `cost` we can use the respective `arrow::Int64Builder` and
// `arrow::DoubleBuilder`. For the `cost_components` vector, we need to have two
// builders, a top-level `arrow::ListBuilder` that builds the array of offsets and
// a nested `arrow::DoubleBuilder` that constructs the underlying values array that
// is referenced by the offsets in the former array.
arrow::Status makeBatch(const std::vector<struct data_row>& rows,
                        std::shared_ptr<arrow::RecordBatch>* batch) {
  // The builders are more efficient using
  // arrow::jemalloc::MemoryPool::default_pool() as this can increase the size of
  // the underlying memory regions in-place. At the moment, arrow::jemalloc is only
  // supported on Unix systems, not Windows.
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  Int64Builder id_builder(pool);
  DoubleBuilder cost_builder(pool);
  ListBuilder components_builder(pool, std::make_shared<DoubleBuilder>(pool));
  // The following builder is owned by components_builder.
  DoubleBuilder& cost_components_builder = *(static_cast<DoubleBuilder*>(components_builder.value_builder()));

  // Now we can loop over our existing data and insert it into the builders. The
  // `Append` calls here may fail (e.g. we cannot allocate enough additional memory).
  // Thus we need to check their return values. For more information on these values,
  // check the documentation about `arrow::Status`.
  for (const data_row& row : rows) {
    ARROW_RETURN_NOT_OK(id_builder.Append(row.id));
    ARROW_RETURN_NOT_OK(cost_builder.Append(row.cost));

    // Indicate the start of a new list row. This will memorise the current
    // offset in the values builder.
    ARROW_RETURN_NOT_OK(components_builder.Append());
    // Store the actual values. The final nullptr argument tells the underyling
    // builder that all added values are valid, i.e. non-null.
    ARROW_RETURN_NOT_OK(cost_components_builder.AppendValues(row.cost_components.data(),
                                                             row.cost_components.size()));
  }

  // At the end, we finalise the arrays, declare the (type) schema and combine them
  // into a single `arrow::Table`:
  std::shared_ptr<arrow::Array> id_array;
  ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
  std::shared_ptr<arrow::Array> cost_array;
  ARROW_RETURN_NOT_OK(cost_builder.Finish(&cost_array));
  // No need to invoke cost_components_builder.Finish because it is implied by
  // the parent builder's Finish invocation.
  std::shared_ptr<arrow::Array> cost_components_array;
  ARROW_RETURN_NOT_OK(components_builder.Finish(&cost_components_array));

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
    arrow::field("id", arrow::int64()),
    arrow::field("cost", arrow::float64()),
    arrow::field("cost_components", arrow::list(arrow::float64()))
  };

  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  *batch = arrow::RecordBatch::Make(schema, rows.size(), { id_array, cost_array, cost_components_array });

  return arrow::Status::OK();
}

arrow::Status readRows(const std::shared_ptr<arrow::RecordBatch>& batch,
                       std::vector<struct data_row>* rows) {
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
    arrow::field("id", arrow::int64()), arrow::field("cost", arrow::float64()),
    arrow::field("cost_components", arrow::list(arrow::float64()))
  };
  auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);

  if (!expected_schema->Equals(*batch->schema())) {
    // The table doesn't have the expected schema thus we cannot directly
    // convert it to our target representation.
    return arrow::Status::Invalid("Schemas are not matching!");
  }

  auto ids = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
  auto costs = std::static_pointer_cast<arrow::DoubleArray>(batch->column(1));
  auto cost_components = std::static_pointer_cast<arrow::ListArray>(batch->column(2));
  auto cost_components_values = std::static_pointer_cast<arrow::DoubleArray>(cost_components->values());
  const double* ccv_ptr = cost_components_values->data()->GetValues<double>(1);

  for (int64_t i = 0; i < batch->num_rows(); i++) {
    int64_t id = ids->Value(i);
    double cost = costs->Value(i);
    const double* first = ccv_ptr + cost_components->value_offset(i);
    const double* last = ccv_ptr + cost_components->value_offset(i + 1);
    std::vector<double> components_vec(first, last);
    rows->push_back({ id, cost, components_vec });
  }

  return arrow::Status::OK();
}

TEST(ArrowTest, BasicArrowData) {
  std::vector<data_row> rows = {
    { 1, 1.0, { 1.0 } }, { 2, 2.0, { 1.0, 2.0 } }, { 3, 3.0, { 1.0, 2.0, 3.0 } }
  };

  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::Status status = makeBatch(rows, &batch);
  EXPECT_TRUE(status.ok());

  //   // serialize the batch - (requires arrow IPC module)
  //   std::shared_ptr<arrow::Buffer> serialized_batch;
  //   arrow::MemoryPool* pool = arrow::default_memory_pool();
  //   auto state = arrow::ipc::SerializeRecordBatch(*batch, pool, &serialized_batch);
  //   EXPECT_TRUE(state.ok());

  //   // deserialize the batch
  //   std::shared_ptr<arrow::RecordBatch> result;
  //   arrow::io::BufferReader buf_reader(serialized_batch);
  //   state = arrow::ipc::ReadRecordBatch(batch->schema(), &buf_reader, &result);
  //   EXPECT_TRUE(state.ok());

  LOG(INFO) << "Completed a round trip to serde an arrow batch";
  std::vector<data_row> expected_rows;
  status = readRows(batch, &expected_rows);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(rows.size(), expected_rows.size());
}

} // namespace test
} // namespace memory
} // namespace nebula