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

// All supported types in nebula
// enum class is strong typed enum, lose the strong type
enum Kind {
  // scalar types are in range[1, 10]
  INVALID = 0,
  BOOLEAN = 1,
  TINYINT = 2,
  SMALLINT = 3,
  INTEGER = 4,
  BIGINT = 5,
  REAL = 6,
  DOUBLE = 7,
  // var length types are in range [11, 20]
  VARCHAR = 11,
  // compound types are in range [21, 30]
  ARRAY = 21,
  MAP = 22,
  STRUCT = 23

  // why do we support these types?
  // they can be substituted by string/long/struct respectively
  // it's not about storage, it's about API conversion
  // VARBINARY = 12,
  // TIMESTAMP = 13,
  // UNION = 14
};

/**
 * Define all individual type alias.
 * Some type has more aliases than others.
 */
template <Kind KIND>
class Type;
using BoolType = Type<Kind::BOOLEAN>;
using TinyType = Type<Kind::TINYINT>;
using ByteType = TinyType;
using SmallType = Type<Kind::SMALLINT>;
using ShortType = SmallType;
using IntType = Type<Kind::INTEGER>;
using BigType = Type<Kind::BIGINT>;
using LongType = BigType;
using RealType = Type<Kind::REAL>;
using FloatType = RealType;
using DoubleType = Type<Kind::DOUBLE>;
using VarcharType = Type<Kind::VARCHAR>;
using StringType = VarcharType;
using ArrayType = Type<Kind::ARRAY>;
using ListType = ArrayType;
using MapType = Type<Kind::MAP>;
using StructType = Type<Kind::STRUCT>;
using RowType = StructType;
// define schema as shared pointer of row type
using Schema = std::shared_ptr<RowType>;
// using VarbinaryType = Type<Kind::VARBINARY>;
// using BinaryType = VarbinaryType;
// using TimestampType = Type<Kind::TIMESTAMP>;

/* 
Every type kind has different traits
- Kind const value 
- is primitive or compound type
- type value width: 0-variable length
- type name literals
*/

template <Kind KIND>
struct TypeTraits {};

#define DEFINE_TYPE_TRAITS(NAME, PRIMITIVE, WIDTH) \
  template <>                                      \
  struct TypeTraits<Kind::NAME> {                  \
    static constexpr Kind typeKind = Kind::NAME;   \
    static constexpr bool isPrimitive = PRIMITIVE; \
    static constexpr size_t width = WIDTH;         \
    static constexpr auto name = #NAME;            \
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
DEFINE_TYPE_TRAITS(ARRAY, false, 0)
DEFINE_TYPE_TRAITS(MAP, false, 0)
DEFINE_TYPE_TRAITS(STRUCT, false, 0)
// DEFINE_TYPE_TRAITS(VARBINARY, true, 0)
// DEFINE_TYPE_TRAITS(TIMESTAMP, true, 8)

#undef DEFINE_TYPE_TRAITS

#define KIND_NAME_DISPATCH(KIND)         \
  case Kind::KIND: {                     \
    return TypeTraits<Kind::KIND>::name; \
  }

static auto KIND_NAME(Kind kind)
  -> std::string {
  switch (kind) {
    KIND_NAME_DISPATCH(BOOLEAN)
    KIND_NAME_DISPATCH(TINYINT)
    KIND_NAME_DISPATCH(SMALLINT)
    KIND_NAME_DISPATCH(INTEGER)
    KIND_NAME_DISPATCH(BIGINT)
    KIND_NAME_DISPATCH(REAL)
    KIND_NAME_DISPATCH(DOUBLE)
    KIND_NAME_DISPATCH(VARCHAR)
    KIND_NAME_DISPATCH(ARRAY)
    KIND_NAME_DISPATCH(MAP)
    KIND_NAME_DISPATCH(STRUCT)
  default:
    throw NebulaException("Unsupported type in KIND_NAME.");
  }
}

#undef KIND_NAME_DISPATCH

// a base properties of a type
class TypeBase {
public:
  // methods to fetch runtime properties
  inline const std::string& name() const {
    return name_;
  }

  // virtual method to get kind of real type
  virtual const Kind k() const = 0;

public:
  static constexpr bool isCompound(Kind kind) {
    return kind == Kind::ARRAY || kind == Kind::MAP || kind == Kind::STRUCT;
  }

  static constexpr bool isScalar(Kind kind) {
    return kind > 0 && kind <= 10;
  }

protected:
  TypeBase(const std::string& name) : name_{ name } {}
  virtual ~TypeBase() = default;
  std::string name_;
};

// define a type node - shared between TreeBase and TypeBase
using TypeNode = std::shared_ptr<TypeBase>;

// every type is templated by a KIND
// We need an abstract type to do generic operations
template <Kind KIND>
class Type : public TypeBase, public Tree<Type<KIND>*> {
public:
  using TType = Type<KIND>;
  // TODO(cao): use safer std::weak_ptr here?
  using PType = TType*;
  virtual ~Type() = default;

  // different KIND has different create method
  // primitive types
  // We put a default bool "OK" here so that it can be disabled for non-primitive types
  template <bool OK = true>
  static auto create(const std::string& name)
    -> typename std::enable_if<TypeTraits<KIND>::isPrimitive && OK, TType>::type {
    return TType(name);
  }

  // Array type
  template <Kind K, bool OK = true>
  static auto create(const std::string& name,
                     const std::shared_ptr<Type<K>>& child)
    -> typename std::enable_if<(TypeTraits<KIND>::typeKind == Kind::ARRAY) && OK, TType>::type {
    auto type = TType(name);
    auto childNode = std::static_pointer_cast<Tree<Type<K>*>>(child);
    const auto& added = type.addChild(childNode);

    // compare the address of these two object
    N_ENSURE_EQ(reinterpret_cast<std::uintptr_t>(added.value()),
                reinterpret_cast<std::uintptr_t>(child->value()),
                "same object with the same address");

    // copy elision
    return type;
  }

  // Map type
  template <Kind K, Kind V, bool OK = true>
  static auto create(const std::string& name,
                     const std::shared_ptr<Type<K>>& key,
                     const std::shared_ptr<Type<V>>& value)
    -> typename std::enable_if<(TypeTraits<KIND>::typeKind == Kind::MAP) && OK, TType>::type {
    auto type = TType(name);
    type.addChild(std::static_pointer_cast<Tree<Type<K>*>>(key));
    type.addChild(std::static_pointer_cast<Tree<Type<V>*>>(value));

    // compare the address of these two object
    N_ENSURE_EQ(type.size(), 2, "only 2 children allowed")

    // copy elision
    return type;
  }

  // Struct type (parameter pack)
  template <bool OK = TypeTraits<KIND>::typeKind == Kind::STRUCT, Kind... K>
  static auto create(const std::string& name, const std::shared_ptr<Type<K>>&... fields)
    -> typename std::enable_if<OK, TType>::type {
    auto type = TType(name);
    type.addChildren(std::static_pointer_cast<Tree<Type<K>*>>(fields)...);

    // pack expansion by sizeof
    N_ENSURE_EQ(type.size(), sizeof...(fields), "all children added");

    // add all fields
    return type;
  }

  // A generic way to provide type creation
  static auto create(const std::string& name, const std::vector<TreeNode>& children)
    -> TreeNode {
    return TreeNode(new TType(name, children));
  }

public:
  TypeNode childType(size_t index) {
    // Tip: "this->" is needed when accessing base's member and base is a template.
    TreeNode node = this->childAt(index);
    return std::dynamic_pointer_cast<TypeBase>(node);
  }

public:
  // proxy of static properties
  static constexpr auto kind = TypeTraits<KIND>::typeKind;
  static constexpr auto type = TypeTraits<KIND>::name;
  static constexpr auto isPrimitive = TypeTraits<KIND>::isPrimitive;
  static constexpr auto isFixedWidth = TypeTraits<KIND>::width > 0;
  static constexpr auto width = TypeTraits<KIND>::width;

  inline std::string toString() const {
    return fmt::format("[name={0}, width={1}]", name(), width);
  }

  inline size_t id() const {
    // TODO(cao): generate identifier for current type
    return static_cast<size_t>(kind);
  }

  inline const Kind k() const {
    return kind;
  }

  inline const TypeBase& base() const {
    return *this;
  }

protected:
  Type(const std::string& name, const std::vector<TreeNode>& children)
    : TypeBase(name), Tree<PType>(this, children) {}
  Type(const std::string& name) : TypeBase(name), Tree<PType>(this) {
    LOG(INFO) << "Construct a type" << toString();
  }
};

} // namespace type
} // namespace nebula