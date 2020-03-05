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

#include "Base.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace dsl {

using nebula::type::Kind;
using nebula::type::TreeNode;
using nebula::type::TypeBase;
using nebula::type::TypeTraits;

TreeNode Expression::typeCreate(Kind kind, const std::string& alias) {
#define TYPE_CREATE_NODE(KIND, TYPE)              \
  case nebula::type::KIND: {                      \
    return nebula::type::TYPE::createTree(alias); \
  }

  // no result should have the final type KIND
  switch (kind) {
    TYPE_CREATE_NODE(BOOLEAN, BoolType)
    TYPE_CREATE_NODE(TINYINT, ByteType)
    TYPE_CREATE_NODE(SMALLINT, ShortType)
    TYPE_CREATE_NODE(INTEGER, IntType)
    TYPE_CREATE_NODE(BIGINT, LongType)
    TYPE_CREATE_NODE(REAL, FloatType)
    TYPE_CREATE_NODE(DOUBLE, DoubleType)
    TYPE_CREATE_NODE(INT128, Int128Type)
    TYPE_CREATE_NODE(VARCHAR, StringType)
  default:
    throw NException(fmt::format("Not supported type {0}", TypeBase::kname(kind)));
  }
#undef TYPE_CREATE_NODE
}

#define CROSS_COMBINE4_FOR(M, OP, F, K1, K2, K3, K4) \
  M(OP, K1, K1, F),                                  \
    M(OP, K1, K2, F),                                \
    M(OP, K1, K3, F),                                \
    M(OP, K1, K4, F),                                \
    M(OP, K2, K1, F),                                \
    M(OP, K2, K2, F),                                \
    M(OP, K2, K3, F),                                \
    M(OP, K2, K4, F),                                \
    M(OP, K3, K1, F),                                \
    M(OP, K3, K2, F),                                \
    M(OP, K3, K3, F),                                \
    M(OP, K3, K4, F),                                \
    M(OP, K4, K1, F),                                \
    M(OP, K4, K2, F),                                \
    M(OP, K4, K3, F),                                \
    M(OP, K4, K4, F)

#define CROSS_COMBINE6_FOR(M, OP, F, K1, K2, K3, K4, K5, K6) \
  CROSS_COMBINE4_FOR(M, OP, F, K1, K2, K3, K4),              \
    M(OP, K1, K5, F),                                        \
    M(OP, K1, K6, F),                                        \
    M(OP, K2, K5, F),                                        \
    M(OP, K2, K6, F),                                        \
    M(OP, K3, K5, F),                                        \
    M(OP, K3, K6, F),                                        \
    M(OP, K4, K5, F),                                        \
    M(OP, K4, K6, F),                                        \
    M(OP, K5, K1, F),                                        \
    M(OP, K5, K2, F),                                        \
    M(OP, K5, K3, F),                                        \
    M(OP, K5, K4, F),                                        \
    M(OP, K5, K5, F),                                        \
    M(OP, K5, K6, F),                                        \
    M(OP, K6, K1, F),                                        \
    M(OP, K6, K2, F),                                        \
    M(OP, K6, K3, F),                                        \
    M(OP, K6, K4, F),                                        \
    M(OP, K6, K5, F),                                        \
    M(OP, K6, K6, F)

const arthmetic_forward::Map& arthmetic_forward::map() {

#define MAP_K1_K2(OP, K1, K2, F)                                               \
  {                                                                            \
    std::make_tuple(ArthmeticOp::OP, Kind::K1, Kind::K2),                      \
      nebula::surface::eval::F<                                                \
        TypeTraits<ArthmeticCombination::result(Kind::K1, Kind::K2)>::CppType, \
        TypeTraits<Kind::K1>::CppType,                                         \
        TypeTraits<Kind::K2>::CppType>                                         \
  }

#define CROSS_COMBINE6(...) CROSS_COMBINE6_FOR(MAP_K1_K2, __VA_ARGS__)
#define CROSS_COMBINE4(...) CROSS_COMBINE4_FOR(MAP_K1_K2, __VA_ARGS__)

  static const Map m = {
    // + - * /
    CROSS_COMBINE6(ADD, add, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),
    CROSS_COMBINE6(SUB, sub, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),
    CROSS_COMBINE6(MUL, mul, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),
    CROSS_COMBINE6(DIV, div, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),

    // mod %
    CROSS_COMBINE4(MOD, mod, TINYINT, SMALLINT, INTEGER, BIGINT)
  };

#undef CROSS_COMBINE4
#undef CROSS_COMBINE6
#undef MAP_K1_K2

  return m;
}

std::unique_ptr<nebula::surface::eval::ValueEval>
  arthmetic_forward::operator()(
    ArthmeticOp op,
    nebula::type::Kind k1,
    nebula::type::Kind k2,
    std::unique_ptr<nebula::surface::eval::ValueEval> v1,
    std::unique_ptr<nebula::surface::eval::ValueEval> v2) {
  // look up map
  const auto& m = map();
  auto itr = m.find(std::make_tuple(op, k1, k2));
  if (itr == m.end()) {
    throw NException("arthmetic function is not defined");
  }

  return itr->second(std::move(v1), std::move(v2));
}

///////////////////////////////////////////////////////////////////////////////////////////////////

const logical_forward::Map& logical_forward::map() {
#define MAP_K1_K2(OP, K1, K2, F)                        \
  { std::make_tuple(LogicalOp::OP, Kind::K1, Kind::K2), \
    nebula::surface::eval::F<TypeTraits<Kind::K1>::CppType, TypeTraits<Kind::K1>::CppType> }

#define CROSS_COMBINE6(...) CROSS_COMBINE6_FOR(MAP_K1_K2, __VA_ARGS__)

  static const Map m = {
    // AND, OR only accept bool operands
    MAP_K1_K2(AND, BOOLEAN, BOOLEAN, band),
    MAP_K1_K2(OR, BOOLEAN, BOOLEAN, bor),

    // EQ
    MAP_K1_K2(EQ, BOOLEAN, BOOLEAN, eq),
    MAP_K1_K2(EQ, VARCHAR, VARCHAR, eq),
    CROSS_COMBINE6(EQ, eq, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),

    // NEQ
    MAP_K1_K2(NEQ, BOOLEAN, BOOLEAN, neq),
    MAP_K1_K2(NEQ, VARCHAR, VARCHAR, neq),
    CROSS_COMBINE6(NEQ, neq, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),

    // GT
    MAP_K1_K2(GT, VARCHAR, VARCHAR, gt),
    CROSS_COMBINE6(GT, gt, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),

    // GE
    MAP_K1_K2(GE, VARCHAR, VARCHAR, ge),
    CROSS_COMBINE6(GE, ge, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),

    // LT
    MAP_K1_K2(LT, VARCHAR, VARCHAR, lt),
    CROSS_COMBINE6(LT, lt, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE),

    // LE
    MAP_K1_K2(LE, VARCHAR, VARCHAR, le),
    CROSS_COMBINE6(LE, le, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE)
  };

#undef CROSS_COMBINE6
#undef MAP_K1_K2

  return m;
}

std::unique_ptr<nebula::surface::eval::ValueEval> logical_forward::operator()(
  LogicalOp op,
  nebula::type::Kind k1,
  nebula::type::Kind k2,
  std::unique_ptr<nebula::surface::eval::ValueEval> v1,
  std::unique_ptr<nebula::surface::eval::ValueEval> v2) {
  // look up map
  const auto& m = map();
  auto itr = m.find(std::make_tuple(op, k1, k2));
  if (itr == m.end()) {
    throw NException(fmt::format("logical function '{0}({1}, {2})' is not defined.",
                                 (int)op, TypeBase::kname(k1), TypeBase::kname(k2)));
  }

  return itr->second(std::move(v1), std::move(v2));
}

#undef CROSS_COMBINE6_FOR

} // namespace dsl
} // namespace api
} // namespace nebula