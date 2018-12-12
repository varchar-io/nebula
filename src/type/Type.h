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

#include "Tree.h"

namespace nebula {
namespace type {
enum class Kind {
  INVALID = 0,
  BOOLEAN = 1,
  TINYINT = 2,
  SMALLINT = 3,
  INTEGER = 4,
  BIGINT = 5,
  REAL = 6,
  DOUBLE = 7,
  VARCHAR = 8,
  VARBINARY = 9,
  TIMESTAMP = 10,
  ARRAY = 11,
  MAP = 12,
  STRUCT = 13,
  UNION = 14
};

/* 
Every type kind has different traits
- Kind const value 
- is primitive or compound type
- type value width: 0-variable length
- type name literals
*/
template <Kind KIND>
struct TypeTraits {
};

#define DEFINE_TYPE_TRAITS(NAME, PRIMITIVE, WIDTH) \
  template <>                                      \
  struct TypeTraits<Kind::NAME> {                  \
    static constexpr Kind typeKind = Kind::NAME;   \
    static constexpr bool isPrimitive = PRIMITIVE; \
    static constexpr size_t width = WIDTH;         \
    static constexpr auto name = "##NAME";         \
  };

// define all traits for each KIND
DEFINE_TYPE_TRAITS(BOOLEAN, true, 1)
DEFINE_TYPE_TRAITS(TINYINT, true, 1)
DEFINE_TYPE_TRAITS(SMALLINT, true, 2)
DEFINE_TYPE_TRAITS(INTEGER, true, 4)
DEFINE_TYPE_TRAITS(BIGINT, true, 8)
DEFINE_TYPE_TRAITS(REAL, true, 4)
DEFINE_TYPE_TRAITS(DOUBLE, true, 8)
DEFINE_TYPE_TRAITS(VARCHAR, true, 0)
DEFINE_TYPE_TRAITS(VARBINARY, true, 0)
DEFINE_TYPE_TRAITS(TIMESTAMP, true, 8)
DEFINE_TYPE_TRAITS(ARRAY, false, 0)
DEFINE_TYPE_TRAITS(MAP, false, 0)
DEFINE_TYPE_TRAITS(STRUCT, false, 0)
DEFINE_TYPE_TRAITS(UNION, false, 0)

#undef DEFINE_TYPE_TRAITS

// We need an abstract type to do generic operations
template <Kind KIND>
class Type : public Tree<Type<KIND>> {
public:
  virtual ~Type() = default;

public:
  std::string toString() const;
  //   Kind kind() const {
  //     return typename TypeTraits<KIND>::typeKind;
  //   }

  //   bool isPrimitive() const {
  //     return typename TypeTraits<KIND>::isPrimitive;
  //   }

  //   bool isFixedWidth() const {
  //     return typename TypeTraits<KIND>::width > 0;
  //   }

  //   size_t width() const {
  //     return typename TypeTraits<KIND>::width;
  //   }
};

} // namespace type
} // namespace nebula