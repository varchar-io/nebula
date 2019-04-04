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
#include <unordered_map>
#include "common/Errors.h"
#include "execution/eval/UDF.h"
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

////////////////////////////////////////// FORWARD ARTH  //////////////////////////////////////////
struct arthmetic_forward {
  using Key = std::tuple<ArthmeticOp, nebula::type::Kind, nebula::type::Kind>;
  using Value = std::function<
    std::unique_ptr<nebula::execution::eval::ValueEval>(
      std::unique_ptr<nebula::execution::eval::ValueEval>,
      std::unique_ptr<nebula::execution::eval::ValueEval>)>;
  struct Hash {
    size_t operator()(const Key& k) const {
      return ((size_t)std::get<0>(k) << 32) | (std::get<1>(k) << 16) | (std::get<2>(k));
    }
  };
  using Map = std::unordered_map<Key, Value, Hash>;

  static const Map& map();

  std::unique_ptr<nebula::execution::eval::ValueEval> operator()(
    ArthmeticOp op,
    nebula::type::Kind k1,
    nebula::type::Kind k2,
    std::unique_ptr<nebula::execution::eval::ValueEval> v1,
    std::unique_ptr<nebula::execution::eval::ValueEval> v2);
};

////////////////////////////////////////// FORWARD LOGI  //////////////////////////////////////////
struct logical_forward {
  using Key = std::tuple<LogicalOp, nebula::type::Kind, nebula::type::Kind>;
  using Value = std::function<
    std::unique_ptr<nebula::execution::eval::ValueEval>(
      std::unique_ptr<nebula::execution::eval::ValueEval>,
      std::unique_ptr<nebula::execution::eval::ValueEval>)>;
  struct Hash {
    size_t operator()(const Key& k) const {
      return ((size_t)std::get<0>(k) << 32) | (std::get<1>(k) << 16) | (std::get<2>(k));
    }
  };
  using Map = std::unordered_map<Key, Value, Hash>;

  static const Map& map();

  std::unique_ptr<nebula::execution::eval::ValueEval> operator()(
    LogicalOp op,
    nebula::type::Kind k1,
    nebula::type::Kind k2,
    std::unique_ptr<nebula::execution::eval::ValueEval> v1,
    std::unique_ptr<nebula::execution::eval::ValueEval> v2);
};

} // namespace dsl
} // namespace api
} // namespace nebula