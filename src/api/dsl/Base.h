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

#include <algorithm>
#include <array>
#include "api/UDF.h"
#include "common/Errors.h"
#include "glog/logging.h"
#include "meta/Table.h"
#include "type/Tree.h"

/**
 * Define expressions used in the nebula DSL.
 */
namespace nebula {
namespace api {
namespace dsl {

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
  GT,
  GE,
  LT,
  LE,
  AND,
  OR
};

#ifndef THIS_TYPE
#define THIS_TYPE typename std::remove_reference<decltype(*this)>::type
#endif

#ifndef IS_T_LITERAL
#define IS_T_LITERAL(T) std::is_same<char*, std::decay_t<T>>::value
#endif

class ArthmeticCombination {
private:
  static constexpr std::array<nebula::type::Kind, 6> numbers{
    nebula::type::Kind::TINYINT,
    nebula::type::Kind::SMALLINT,
    nebula::type::Kind::INTEGER,
    nebula::type::Kind::BIGINT,
    nebula::type::Kind::REAL,
    nebula::type::Kind::DOUBLE
  };

  static constexpr size_t index(nebula::type::Kind k1, nebula::type::Kind k2) noexcept {
    constexpr size_t size = numbers.size();
    return (k1 - nebula::type::Kind::TINYINT) * size + (k2 - nebula::type::Kind::TINYINT);
  }

  // closure to initialize the map
  static constexpr std::array<nebula::type::Kind, 36> map = []() {
    constexpr size_t size = numbers.size();
    std::array<nebula::type::Kind, size * size> m{};
    int index = 0;
    for (auto i = 0; i < size; ++i) {
      for (auto j = 0; j < size; ++j) {
        m[i * size + j] = std::max(numbers[i], numbers[j]);
      }
    }

    return m;
  }();

public:
  static constexpr nebula::type::Kind result(nebula::type::Kind k1, nebula::type::Kind k2) noexcept {
    return map[index(k1, k2)];
  }
};

// TODO(cao) - This explosion of the switch case probably will slow down compilation time a lot
// Looking for a way to speed this up

#define N_DEFAULT_CASE_EXP \
  default:                 \
    throw NException("type not supported");

#define N_CASE_3RD_TYPE(F, T1, T2, KIND, T, ...) \
  case nebula::type::Kind::KIND: {               \
    return F<T1, T2, T>(__VA_ARGS__);            \
  }

// NOTE: we don't use N_CASE_3RD_TYPE here for expansion speed
// compilation speed is to slow
#define N_CASE_2ND_TYPE(F, T1, KIND, T, K3, ...)                \
  case nebula::type::Kind::KIND: {                              \
    switch (K3) {                                               \
      N_CASE_3RD_TYPE(F, T1, T, TINYINT, int8_t, __VA_ARGS__)   \
      N_CASE_3RD_TYPE(F, T1, T, SMALLINT, int16_t, __VA_ARGS__) \
      N_CASE_3RD_TYPE(F, T1, T, INTEGER, int32_t, __VA_ARGS__)  \
      N_CASE_3RD_TYPE(F, T1, T, BIGINT, int64_t, __VA_ARGS__)   \
      N_CASE_3RD_TYPE(F, T1, T, REAL, float, __VA_ARGS__)       \
      N_CASE_3RD_TYPE(F, T1, T, DOUBLE, double, __VA_ARGS__)    \
      N_DEFAULT_CASE_EXP                                        \
    }                                                           \
  }

#define N_CASE_1ST_TYPE(F, KIND, T, K2, K3, ...)                \
  case nebula::type::Kind::KIND: {                              \
    switch (K2) {                                               \
      N_CASE_2ND_TYPE(F, T, TINYINT, int8_t, K3, __VA_ARGS__)   \
      N_CASE_2ND_TYPE(F, T, SMALLINT, int16_t, K3, __VA_ARGS__) \
      N_CASE_2ND_TYPE(F, T, INTEGER, int32_t, K3, __VA_ARGS__)  \
      N_CASE_2ND_TYPE(F, T, BIGINT, int64_t, K3, __VA_ARGS__)   \
      N_CASE_2ND_TYPE(F, T, REAL, float, K3, __VA_ARGS__)       \
      N_CASE_2ND_TYPE(F, T, DOUBLE, double, K3, __VA_ARGS__)    \
      N_DEFAULT_CASE_EXP                                        \
    }                                                           \
  }

// closure to translate 3 kinds into 3 types in the func template and call it
#define N_FUNC_3_BY_KIND(FUNC_3, K1, K2, K3, ...)                     \
  [&]() {                                                             \
    switch (K1) {                                                     \
      N_CASE_1ST_TYPE(FUNC_3, TINYINT, int8_t, K2, K3, __VA_ARGS__)   \
      N_CASE_1ST_TYPE(FUNC_3, SMALLINT, int16_t, K2, K3, __VA_ARGS__) \
      N_CASE_1ST_TYPE(FUNC_3, INTEGER, int32_t, K2, K3, __VA_ARGS__)  \
      N_CASE_1ST_TYPE(FUNC_3, BIGINT, int64_t, K2, K3, __VA_ARGS__)   \
      N_CASE_1ST_TYPE(FUNC_3, REAL, float, K2, K3, __VA_ARGS__)       \
      N_CASE_1ST_TYPE(FUNC_3, DOUBLE, double, K2, K3, __VA_ARGS__)    \
      N_DEFAULT_CASE_EXP                                              \
    }                                                                 \
  }()

// NOTE: NO floating values expansion
#define NF_CASE_2ND_TYPE(F, T1, KIND, T, K3, ...)               \
  case nebula::type::Kind::KIND: {                              \
    switch (K3) {                                               \
      N_CASE_3RD_TYPE(F, T1, T, TINYINT, int8_t, __VA_ARGS__)   \
      N_CASE_3RD_TYPE(F, T1, T, SMALLINT, int16_t, __VA_ARGS__) \
      N_CASE_3RD_TYPE(F, T1, T, INTEGER, int32_t, __VA_ARGS__)  \
      N_CASE_3RD_TYPE(F, T1, T, BIGINT, int64_t, __VA_ARGS__)   \
      N_DEFAULT_CASE_EXP                                        \
    }                                                           \
  }

#define NF_CASE_1ST_TYPE(F, KIND, T, K2, K3, ...)                \
  case nebula::type::Kind::KIND: {                               \
    switch (K2) {                                                \
      NF_CASE_2ND_TYPE(F, T, TINYINT, int8_t, K3, __VA_ARGS__)   \
      NF_CASE_2ND_TYPE(F, T, SMALLINT, int16_t, K3, __VA_ARGS__) \
      NF_CASE_2ND_TYPE(F, T, INTEGER, int32_t, K3, __VA_ARGS__)  \
      NF_CASE_2ND_TYPE(F, T, BIGINT, int64_t, K3, __VA_ARGS__)   \
      N_DEFAULT_CASE_EXP                                         \
    }                                                            \
  }

// closure to translate 3 kinds into 3 types in the func template and call it
#define NF_FUNC_3_BY_KIND(FUNC_3, K1, K2, K3, ...)                     \
  [&]() {                                                              \
    switch (K1) {                                                      \
      NF_CASE_1ST_TYPE(FUNC_3, TINYINT, int8_t, K2, K3, __VA_ARGS__)   \
      NF_CASE_1ST_TYPE(FUNC_3, SMALLINT, int16_t, K2, K3, __VA_ARGS__) \
      NF_CASE_1ST_TYPE(FUNC_3, INTEGER, int32_t, K2, K3, __VA_ARGS__)  \
      NF_CASE_1ST_TYPE(FUNC_3, BIGINT, int64_t, K2, K3, __VA_ARGS__)   \
      N_DEFAULT_CASE_EXP                                               \
    }                                                                  \
  }()

// TODO(cao) - VARCHAR type should be included in logical expression
// because they can be used for comparison and other logical ops
#define N_CASE_K2(F, T1, KIND, T, ...) \
  case nebula::type::Kind::KIND: {     \
    return F<T1, T>(__VA_ARGS__);      \
  }

#define N_CASE_K1(F, KIND, T, K2, ...)                \
  case nebula::type::Kind::KIND: {                    \
    switch (K2) {                                     \
      N_CASE_K2(F, T, BOOLEAN, bool, __VA_ARGS__)     \
      N_CASE_K2(F, T, TINYINT, int8_t, __VA_ARGS__)   \
      N_CASE_K2(F, T, SMALLINT, int16_t, __VA_ARGS__) \
      N_CASE_K2(F, T, INTEGER, int32_t, __VA_ARGS__)  \
      N_CASE_K2(F, T, BIGINT, int64_t, __VA_ARGS__)   \
      N_CASE_K2(F, T, REAL, float, __VA_ARGS__)       \
      N_CASE_K2(F, T, DOUBLE, double, __VA_ARGS__)    \
      N_DEFAULT_CASE_EXP                              \
    }                                                 \
  }

#define N_FUNC_2_BY_KIND(FUNC_2, K1, K2, ...)               \
  [&]() {                                                   \
    switch (K1) {                                           \
      N_CASE_K1(FUNC_2, BOOLEAN, bool, K2, __VA_ARGS__)     \
      N_CASE_K1(FUNC_2, TINYINT, int8_t, K2, __VA_ARGS__)   \
      N_CASE_K1(FUNC_2, SMALLINT, int16_t, K2, __VA_ARGS__) \
      N_CASE_K1(FUNC_2, INTEGER, int32_t, K2, __VA_ARGS__)  \
      N_CASE_K1(FUNC_2, BIGINT, int64_t, K2, __VA_ARGS__)   \
      N_CASE_K1(FUNC_2, REAL, float, K2, __VA_ARGS__)       \
      N_CASE_K1(FUNC_2, DOUBLE, double, K2, __VA_ARGS__)    \
    case nebula::type::Kind::VARCHAR: {                     \
      return FUNC_2<std::string, std::string>(__VA_ARGS__); \
    }                                                       \
      N_DEFAULT_CASE_EXP                                    \
    }                                                       \
  }()

////////////////////////////////////////// FORWARD ARTH  //////////////////////////////////////////
template <ArthmeticOp OP>
static std::unique_ptr<nebula::execution::eval::ValueEval> forward3(
  nebula::type::Kind k1,
  nebula::type::Kind k2,
  nebula::type::Kind k3,
  std::unique_ptr<nebula::execution::eval::ValueEval> v1,
  std::unique_ptr<nebula::execution::eval::ValueEval> v2) {
  throw NException("Arthmetic op not implemented in value eval");
}

#define FORWARD_EVAL(MC, OP, NAME)                                                                                    \
  template <>                                                                                                         \
  std::unique_ptr<nebula::execution::eval::ValueEval> forward3<ArthmeticOp::OP>(                                      \
    nebula::type::Kind k1, nebula::type::Kind k2, nebula::type::Kind k3,                                              \
    std::unique_ptr<nebula::execution::eval::ValueEval> v1, std::unique_ptr<nebula::execution::eval::ValueEval> v2) { \
    return MC(nebula::execution::eval::NAME, k1, k2, k3, std::move(v1), std::move(v2));                               \
  }

FORWARD_EVAL(N_FUNC_3_BY_KIND, ADD, add)
FORWARD_EVAL(N_FUNC_3_BY_KIND, SUB, sub)
FORWARD_EVAL(N_FUNC_3_BY_KIND, MUL, mul)
FORWARD_EVAL(N_FUNC_3_BY_KIND, DIV, div)
FORWARD_EVAL(NF_FUNC_3_BY_KIND, MOD, mod)

#undef FORWARD_EVAL

////////////////////////////////////////// FORWARD LOGI  //////////////////////////////////////////
template <LogicalOp OP>
static std::unique_ptr<nebula::execution::eval::ValueEval> forward2(
  nebula::type::Kind k1,
  nebula::type::Kind k2,
  std::unique_ptr<nebula::execution::eval::ValueEval> v1,
  std::unique_ptr<nebula::execution::eval::ValueEval> v2) {
  throw NException("Logical op not implemented in value eval");
}

#define FORWARD_EVAL(OP, NAME)                                                                    \
  template <>                                                                                     \
  std::unique_ptr<nebula::execution::eval::ValueEval> forward2<LogicalOp::OP>(                    \
    nebula::type::Kind k1,                                                                        \
    nebula::type::Kind k2,                                                                        \
    std::unique_ptr<nebula::execution::eval::ValueEval> v1,                                       \
    std::unique_ptr<nebula::execution::eval::ValueEval> v2) {                                     \
    return N_FUNC_2_BY_KIND(nebula::execution::eval::NAME, k1, k2, std::move(v1), std::move(v2)); \
  }

FORWARD_EVAL(EQ, eq)
FORWARD_EVAL(GT, gt)
FORWARD_EVAL(GE, ge)
FORWARD_EVAL(LT, lt)
FORWARD_EVAL(LE, le)

#undef FORWARD_EVAL

template <>
std::unique_ptr<nebula::execution::eval::ValueEval> forward2<LogicalOp::AND>(
  nebula::type::Kind k1,
  nebula::type::Kind k2,
  std::unique_ptr<nebula::execution::eval::ValueEval> v1,
  std::unique_ptr<nebula::execution::eval::ValueEval> v2) {
  return nebula::execution::eval::band<bool, bool>(std::move(v1), std::move(v2));
}

template <>
std::unique_ptr<nebula::execution::eval::ValueEval> forward2<LogicalOp::OR>(
  nebula::type::Kind k1,
  nebula::type::Kind k2,
  std::unique_ptr<nebula::execution::eval::ValueEval> v1,
  std::unique_ptr<nebula::execution::eval::ValueEval> v2) {
  return nebula::execution::eval::bor<bool, bool>(std::move(v1), std::move(v2));
}

} // namespace dsl
} // namespace api
} // namespace nebula