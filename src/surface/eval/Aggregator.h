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

#pragma once

#include <fmt/format.h>

#include "common/Likely.h"
#include "common/Memory.h"
#include "type/Type.h"

/**
 * Every UDAF will be able to create an aggregator to aggregate desired values in.
 * An aggregator should be stateful means it has suitable serde to read/write itself cross boundary.
 * Also, aggregator should be able to merge the same types for distributed computing.
 */
namespace nebula {
namespace surface {
namespace eval {

class Sketch {
public:
  virtual ~Sketch() = default;

  // optimization: decide if current sektch can fit storage of given size in bytes
  virtual bool fit(size_t) = 0;

  // serialize into a buffer
  virtual size_t serialize(nebula::common::PagedSlice&, size_t) = 0;

  // deserialize from a given buffer, and offset, return total size consumed
  virtual size_t load(nebula::common::PagedSlice&, size_t) = 0;

  // aggregate another sketch
  virtual void mix(const Sketch&) = 0;
};

// aggregator defines its final type and input data type
template <nebula::type::Kind OK, nebula::type::Kind IK>
class Aggregator : public Sketch {
  using InputType = typename nebula::type::TypeTraits<IK>::CppType;
  using OutputType = typename nebula::type::TypeTraits<OK>::CppType;

public:
  virtual ~Aggregator() = default;

  // aggregate an value in
  virtual void merge(InputType) = 0;

  virtual OutputType finalize() = 0;
};

// a sketch/aggregator always have an allocated space for given input type
// if this is provided
template <nebula::type::Kind OK, nebula::type::Kind IK>
struct VoidAggregator : public Aggregator<OK, IK> {
  using InputType = typename nebula::type::TypeTraits<IK>::CppType;
  using OutputType = typename nebula::type::TypeTraits<OK>::CppType;

public:
  VoidAggregator() {}
  virtual ~VoidAggregator() = default;

  // aggregate an value in
  inline virtual void merge(InputType) override {
  }

  // aggregate another aggregator
  inline virtual void mix(const nebula::surface::eval::Sketch&) override {
  }

  inline virtual OutputType finalize() override {
    return nebula::type::TypeDetect<OutputType>::value;
  }

  // serialize into a buffer
  inline virtual size_t serialize(nebula::common::PagedSlice&, size_t) override {
    return 0;
  }

  // deserialize from a given buffer, and bin size
  inline virtual size_t load(nebula::common::PagedSlice&, size_t) override {
    return 0;
  }

  inline virtual bool fit(size_t) override {
    return true;
  }
};

/** BELOW two macro provides global templating for switch/case 
 * with full combination of input/output types
 * 
 * this two macro are used to support input/output combination
 * it requires use client to implement LOGIC_BY_IO macro/function
 * and of course providing input/output types using type::Kind
**/

// TODO: support aggregation on input of string type
// temporarly disabling it LOGIC_BY_IO(O, VARCHAR)
#ifndef ITERATE_BY_IO_CASE
#define ITERATE_BY_IO_CASE(O, IT)                                     \
  case Kind::O: {                                                     \
    switch (IT) {                                                     \
      LOGIC_BY_IO(O, BOOLEAN)                                         \
      LOGIC_BY_IO(O, TINYINT)                                         \
      LOGIC_BY_IO(O, SMALLINT)                                        \
      LOGIC_BY_IO(O, INTEGER)                                         \
      LOGIC_BY_IO(O, BIGINT)                                          \
      LOGIC_BY_IO(O, REAL)                                            \
      LOGIC_BY_IO(O, DOUBLE)                                          \
      LOGIC_BY_IO(O, INT128)                                          \
    default:                                                          \
      LOG(ERROR) << "Unsupported aggregation types (#O, " << (int)IT; \
      break;                                                          \
    }                                                                 \
    break;                                                            \
  }
#endif

#ifndef ITERATE_BY_IO
#define ITERATE_BY_IO(OT, IT)                                         \
  switch (OT) {                                                       \
    ITERATE_BY_IO_CASE(BOOLEAN, IT)                                   \
    ITERATE_BY_IO_CASE(TINYINT, IT)                                   \
    ITERATE_BY_IO_CASE(SMALLINT, IT)                                  \
    ITERATE_BY_IO_CASE(INTEGER, IT)                                   \
    ITERATE_BY_IO_CASE(BIGINT, IT)                                    \
    ITERATE_BY_IO_CASE(REAL, IT)                                      \
    ITERATE_BY_IO_CASE(DOUBLE, IT)                                    \
    ITERATE_BY_IO_CASE(INT128, IT)                                    \
    ITERATE_BY_IO_CASE(VARCHAR, IT)                                   \
  default:                                                            \
    LOG(ERROR) << "Unsupported aggregation output type: " << (int)OT; \
    break;                                                            \
  }
#endif

} // namespace eval
} // namespace surface
} // namespace nebula