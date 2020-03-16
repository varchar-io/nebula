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

/**
 * Define basic operation types that can be used by expressions and value evaluations.
 */
namespace nebula {
namespace surface {
namespace eval {

// define all supported expression types
enum ExpressionType {
  UNKNOWN = 0,
  CONSTANT,
  COLUMN,
  ARTHMETIC,
  LOGICAL,
  FUNCTION
};

// supported arthmetic operations
enum class ArthmeticOp {
  ADD,
  SUB,
  MUL,
  DIV,
  MOD
};

// supported logical operations
enum class LogicalOp {
  EQ,
  NEQ,
  GT,
  GE,
  LT,
  LE,
  AND,
  OR
};

template <ArthmeticOp>
struct ArthmeticOpTraits {};

template <LogicalOp>
struct LogicalOpTraits {};

#define DEFINE_TRAITS(TRAITS, KIND, API, SIGN) \
  template <>                                  \
  struct TRAITS<KIND> {                        \
    static constexpr auto api = #API;          \
    static constexpr auto sign = #SIGN;        \
  };

// define arthmetic operation traits
DEFINE_TRAITS(ArthmeticOpTraits, ArthmeticOp::ADD, add, +)
DEFINE_TRAITS(ArthmeticOpTraits, ArthmeticOp::SUB, sub, -)
DEFINE_TRAITS(ArthmeticOpTraits, ArthmeticOp::MUL, mul, *)
DEFINE_TRAITS(ArthmeticOpTraits, ArthmeticOp::DIV, div, /)
DEFINE_TRAITS(ArthmeticOpTraits, ArthmeticOp::MOD, mod, %)

// define logical operation traits
DEFINE_TRAITS(LogicalOpTraits, LogicalOp::EQ, eq, ==)
DEFINE_TRAITS(LogicalOpTraits, LogicalOp::NEQ, eq, !=)
DEFINE_TRAITS(LogicalOpTraits, LogicalOp::GT, gt, >)
DEFINE_TRAITS(LogicalOpTraits, LogicalOp::GE, ge, >=)
DEFINE_TRAITS(LogicalOpTraits, LogicalOp::LT, lt, <)
DEFINE_TRAITS(LogicalOpTraits, LogicalOp::LE, le, <=)
DEFINE_TRAITS(LogicalOpTraits, LogicalOp::AND, band, &&)
DEFINE_TRAITS(LogicalOpTraits, LogicalOp::OR, bor, ||)

#undef DEFINE_TRAITS

} // namespace eval
} // namespace surface
} // namespace nebula