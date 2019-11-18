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

#include "RowCursorSerde.h"

#include <yorel/yomm2/cute.hpp>

#include "execution/core/BlockExecutor.h"
#include "memory/keyed/FlatRowCursor.h"
#include "type/Serde.h"

/**
 * This header defines logic to convert a RowCursor to a serializable data format.
 * So that an RowCursor consumer can use this extension methods to do serialization work.
 * 
 * The extension requires namespace scope definition.
 */
namespace nebula {
namespace execution {
namespace serde {

#ifdef USE_YOMM2_MD
// register class chain to handle this aspect (AOP)
// A single SamplesCursor may be returned for single block node or Composite cursor returned to chain multiple cursors
register_class(nebula::surface::RowCursor);

// A block executor will be returned by NodeExecutor if it is only one block with aggregation
register_class(nebula::execution::core::BlockExecutor, nebula::surface::RowCursor);

// a sample executor will be returned by NodeExecutor if only one block hit and returns SampleExecutor
register_class(nebula::execution::core::SamplesExecutor, nebula::surface::RowCursor);

// A flat row cursor will be retruned by NodeExecutor if multiple blocks aggregated.
register_class(nebula::memory::keyed::FlatRowCursor, nebula::surface::RowCursor);

// A flat row cursor will be retruned by NodeExecutor if multiple samples chained
register_class(nebula::surface::CompositeRowCursor, nebula::surface::RowCursor);

// MockRowCursor is sub class used for testing
register_class(nebula::surface::MockRowCursor, nebula::surface::RowCursor);

// after a row cursor converted to a flat buffer, its data is moved out
// we can't not use it any more, this is especially true for block executorƒ
declare_method(FlatBufferPtr, asBuffer,
               (yorel::yomm2::virtual_<nebula::surface::RowCursor&>, nebula::type::Schema));

// by default, we need to create a new flat buffer and put all rows in.
define_method(FlatBufferPtr, asBuffer, (nebula::surface::RowCursor & cursor, nebula::type::Schema schema)) {
  auto buffer = std::make_unique<nebula::memory::keyed::FlatBuffer>(schema);
  while (cursor.hasNext()) {
    buffer->add(cursor.next());
  }

  // move this buffer out
  LOG(INFO) << "Serialized a cursor as flat buffer via yomm2";
  return buffer;
}

// for block executor ->
define_method(FlatBufferPtr, asBuffer, (nebula::execution::core::BlockExecutor & b, nebula::type::Schema)) {
  return b.takeResult();
}

// for block executor ->
define_method(FlatBufferPtr, asBuffer, (nebula::memory::keyed::FlatRowCursor & f, nebula::type::Schema)) {
  return f.takeResult();
}

// client module entry point required to call this before using above multi-dispatch methods
void init() {
  yorel::yomm2::update_methods();
}

#else

// by default, if yomm2 is not available or buggy, go with this dynamic cast version as fallback
FlatBufferPtr asBuffer(nebula::surface::RowCursor& cursor, nebula::type::Schema schema) {
  if (auto b = dynamic_cast<nebula::execution::core::BlockExecutor*>(&cursor)) {
    return b->takeResult();
  } else if (auto f = dynamic_cast<nebula::memory::keyed::FlatRowCursor*>(&cursor)) {
    return f->takeResult();
  } else {
    auto buffer = std::make_unique<nebula::memory::keyed::FlatBuffer>(schema);
    while (cursor.hasNext()) {
      buffer->add(cursor.next());
    }

    // move this buffer out
    LOG(INFO) << "Serialized a cursor as flat buffer.";
    return buffer;
  }
}

void init() {
}

#endif
} // namespace serde
} // namespace execution
} // namespace nebula