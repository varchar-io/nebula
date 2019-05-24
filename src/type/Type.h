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

#define DEFINE_TYPE_TRAITS(NAME, PRIMITIVE, WIDTH, CPP) \
  template <>                                           \
  struct TypeTraits<Kind::NAME> {                       \
    static constexpr Kind typeKind = Kind::NAME;        \
    static constexpr bool isPrimitive = PRIMITIVE;      \
    static constexpr size_t width = WIDTH;              \
    static constexpr auto name = #NAME;                 \
    using CppType = CPP;                                \
  };

// define all traits for each KIND, we don't have examples for compound types here
// since they requires runtime composition with child types
DEFINE_TYPE_TRAITS(BOOLEAN, true, 1, bool)
DEFINE_TYPE_TRAITS(TINYINT, true, 1, int8_t)
DEFINE_TYPE_TRAITS(SMALLINT, true, 2, int16_t)
DEFINE_TYPE_TRAITS(INTEGER, true, 4, int32_t)
DEFINE_TYPE_TRAITS(BIGINT, true, 8, int64_t)
DEFINE_TYPE_TRAITS(REAL, true, 4, float)
DEFINE_TYPE_TRAITS(DOUBLE, true, 8, double)
DEFINE_TYPE_TRAITS(VARCHAR, true, 0, std::string_view)
DEFINE_TYPE_TRAITS(ARRAY, false, 0, void)
DEFINE_TYPE_TRAITS(MAP, false, 0, void)
DEFINE_TYPE_TRAITS(STRUCT, false, 0, void)
// DEFINE_TYPE_TRAITS(VARBINARY, true, 0)
// DEFINE_TYPE_TRAITS(TIMESTAMP, true, 8)

#undef DEFINE_TYPE_TRAITS

#define KIND_DISPATCH(KIND, PROP)        \
  case Kind::KIND: {                     \
    return TypeTraits<Kind::KIND>::PROP; \
  }

// a base properties of a type
class TypeBase {
public:
  // methods to fetch runtime properties
  inline const std::string& name() const {
    return name_;
  }

  // virtual method to get kind of real type
  virtual Kind k() const = 0;

public:
#define KIND_NAME_DISPATCH(K) KIND_DISPATCH(K, name)
  static std::string kname(Kind kind) {
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
      throw NException("Unsupported type in kname.");
    }
  }
#undef KIND_NAME_DISPATCH

  static constexpr bool isCompound(Kind kind) noexcept {
    return kind == Kind::ARRAY || kind == Kind::MAP || kind == Kind::STRUCT;
  }

  static constexpr bool isScalar(Kind kind) noexcept {
    return kind > 0 && kind <= 10;
  }

  inline static Kind k(TreeNode node) {
    // every type tree node carries kind in its ID field
    return static_cast<nebula::type::Kind>(node->getId());
  }

protected:
  TypeBase(const std::string& name) : name_{ name } {}
  virtual ~TypeBase() = default;

private:
  std::string name_;
};

#undef KIND_DISPATCH

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
  Type(const std::string& name, const std::vector<TreeNode>& children)
    : TypeBase(name), Tree<PType>(this, children) {}
  Type(const std::string& name) : TypeBase(name), Tree<PType>(this) {}
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
    N_ENSURE_EQ(type.size(), 2, "only 2 children allowed");

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
    return std::dynamic_pointer_cast<TreeBase>(std::make_shared<TType>(name, children));
  }

  static auto createTree(const std::string& name)
    -> TreeNode {
    return std::dynamic_pointer_cast<TreeBase>(std::make_shared<TType>(name));
  }

public:
  TypeNode childType(size_t index) {
    // Tip: "this->" is needed when accessing base's member and base is a template.
    TreeNode node = this->childAt(index);
    return std::dynamic_pointer_cast<TypeBase>(node);
  }

  // look up direct child which matches the name and execute work /function on it
  void onChild(const std::string& name, std::function<void(const TypeNode&)> func) {
    N_ENSURE(KIND == Kind::STRUCT, "only support working on row type");

    // build a field name to data node
    for (size_t i = 0, s = this->size(); i < s; ++i) {
      // name match
      auto columnType = childType(i);
      if (name == columnType->name()) {
        func(columnType);
        break;
      }
    }
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

  inline Kind k() const {
    return kind;
  }

  inline const TypeBase& base() const {
    return *this;
  }
};

///////////////////////////////////////////////////////////////////////////////////////////////////
template <typename T>
struct TypeDetect {};

#define DEFINE_TYPE_DETECT(NT, KN, KT, ST, DV)                                           \
  template <>                                                                            \
  struct TypeDetect<NT> {                                                                \
    using StandardType = ST;                                                             \
    static constexpr Kind kind = Kind::KN;                                               \
    static constexpr auto name = #KN;                                                    \
    static constexpr auto type = [](const std::string& n) { return KT::createTree(n); }; \
    static constexpr ST value = DV;                                                      \
  };

// TODO(cao) - guidelines for nebula API usage
// define all traits for each KIND - incomplete list
// please use remove reference and remove const before using type detect
// std::remove_reference<std::remove_cv<T>::type>::type
DEFINE_TYPE_DETECT(bool, BOOLEAN, BoolType, bool, false)
DEFINE_TYPE_DETECT(int8_t, TINYINT, ByteType, int8_t, 0)
DEFINE_TYPE_DETECT(int16_t, SMALLINT, ShortType, int16_t, 0)
DEFINE_TYPE_DETECT(int32_t, INTEGER, IntType, int32_t, 0)
DEFINE_TYPE_DETECT(int64_t, BIGINT, LongType, int64_t, 0)
DEFINE_TYPE_DETECT(float, REAL, FloatType, float, 0)
DEFINE_TYPE_DETECT(double, DOUBLE, DoubleType, double, 0)
DEFINE_TYPE_DETECT(const char*, VARCHAR, StringType, std::string_view, "")
DEFINE_TYPE_DETECT(char*, VARCHAR, StringType, std::string_view, "")
DEFINE_TYPE_DETECT(std::string_view, VARCHAR, StringType, std::string_view, "")
DEFINE_TYPE_DETECT(std::string, VARCHAR, StringType, std::string_view, "")

#undef DEFINE_TYPE_DETECT

} // namespace type
} // namespace nebula