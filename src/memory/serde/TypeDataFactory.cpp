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

#include "TypeDataFactory.h"
#include "meta/Table.h"

namespace nebula {
namespace memory {
namespace serde {

using nebula::meta::Column;
using nebula::type::Kind;

#define TYPE_DATA_PROXY(KIND, TYPE)                                                    \
  case Kind::KIND: {                                                                   \
    return std::make_unique<TypeDataProxy>(std::make_unique<TYPE>(column, batchSize)); \
  }

std::unique_ptr<TypeDataProxy> TypeDataFactory::createData(
  const nebula::type::TypeBase& type, const Column& column, size_t batchSize) {
  // if current column is a partition column,
  // metadata will record its value and we will not create data stream for it.
  if (column.partition.valid()) {
    return nullptr;
  }

  switch (type.k()) {
    TYPE_DATA_PROXY(BOOLEAN, BoolData)
    TYPE_DATA_PROXY(TINYINT, ByteData)
    TYPE_DATA_PROXY(SMALLINT, ShortData)
    TYPE_DATA_PROXY(INTEGER, IntData)
    TYPE_DATA_PROXY(BIGINT, LongData)
    TYPE_DATA_PROXY(REAL, FloatData)
    TYPE_DATA_PROXY(DOUBLE, DoubleData)
    TYPE_DATA_PROXY(INT128, Int128Data)
    TYPE_DATA_PROXY(VARCHAR, StringData)
    TYPE_DATA_PROXY(ARRAY, EmptyData)
    TYPE_DATA_PROXY(MAP, EmptyData)
    TYPE_DATA_PROXY(STRUCT, EmptyData)
  default:
    // other types has no data
    return nullptr;
  }
}

#undef TYPE_DATA_PROXY

std::unique_ptr<TypeMetadata> TypeDataFactory::createMeta(
  const nebula::type::TypeBase& type, const Column& column) {
  return std::make_unique<TypeMetadata>(type, column);
}

} // namespace serde
} // namespace memory
} // namespace nebula