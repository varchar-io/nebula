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

#include "ValueEval.h"

/**
 * This class is mostly placing all optimizations we can do at block level.
 * What do we have to support these fast decisions?
 * - Histogram: range[min, max], nulls
 * - Bloom Filter: probability
 * - Partition columns
 */
namespace nebula {
namespace surface {
namespace eval {

using nebula::type::Kind;
using nebula::type::TypeTraits;

#define EvalBlock std::function<BlockEval(const Block&)>

// condition "column > C", if max(column) <= C, no records match
// condition "column > C", if min(column) > C, all records match
// if the column is partition column, we use partition values, otherwise use histogram
#define MIN_MAX_COMPARE(KIND, HT, NONE_EXP, ALL_EXP)               \
  using ET = TypeTraits<Kind::KIND>::CppType;                      \
  auto value = c->eval<ET>(ctx);                                   \
  auto min = std::numeric_limits<ET>::max();                       \
  auto max = std::numeric_limits<ET>::lowest();                    \
  auto values = b.partitionValues(name);                           \
  if (values.size() > 0) {                                         \
    for (auto v : values) {                                        \
      auto ev = std::any_cast<ET>(v);                              \
      if (ev < min) {                                              \
        min = ev;                                                  \
      }                                                            \
      if (ev > max) {                                              \
        max = ev;                                                  \
      }                                                            \
    }                                                              \
  } else {                                                         \
    auto histo = std::dynamic_pointer_cast<HT>(b.histogram(name)); \
    min = histo->min();                                            \
    max = histo->max();                                            \
  }                                                                \
  if (NONE_EXP) {                                                  \
    return BlockEval::NONE;                                        \
  }                                                                \
  if (ALL_EXP) {                                                   \
    return BlockEval::ALL;                                         \
  }

#define DISPATCH_CASES(NONE_EXP, ALL_EXP)                      \
  case Kind::TINYINT: {                                        \
    MIN_MAX_COMPARE(TINYINT, IntHistogram, NONE_EXP, ALL_EXP)  \
    break;                                                     \
  }                                                            \
  case Kind::SMALLINT: {                                       \
    MIN_MAX_COMPARE(SMALLINT, IntHistogram, NONE_EXP, ALL_EXP) \
    break;                                                     \
  }                                                            \
  case Kind::INTEGER: {                                        \
    MIN_MAX_COMPARE(INTEGER, IntHistogram, NONE_EXP, ALL_EXP)  \
    break;                                                     \
  }                                                            \
  case Kind::BIGINT: {                                         \
    MIN_MAX_COMPARE(BIGINT, IntHistogram, NONE_EXP, ALL_EXP)   \
    break;                                                     \
  }                                                            \
  case Kind::REAL: {                                           \
    MIN_MAX_COMPARE(REAL, RealHistogram, NONE_EXP, ALL_EXP)    \
    break;                                                     \
  }                                                            \
  case Kind::DOUBLE: {                                         \
    MIN_MAX_COMPARE(DOUBLE, RealHistogram, NONE_EXP, ALL_EXP)  \
    break;                                                     \
  }

// Optimization for case of "column > C"
template <>
EvalBlock buildEvalBlock<LogicalOp::GT>(const std::unique_ptr<ValueEval>& left,
                                        const std::unique_ptr<ValueEval>& right) {
  if (left->expressionType() == ExpressionType::COLUMN
      && right->expressionType() == ExpressionType::CONSTANT) {
    // column expr signature is composed by "F:{col}"
    // const expr signature is compsoed by "C:{col}"
    std::string colName(left->signature().substr(2));
    return [name = std::move(colName), c = right.get()](const Block& b) -> BlockEval {
      EvalContext ctx{ false };
      // logic
      auto ct = b.columnType(name);
      switch (ct->k()) {
        DISPATCH_CASES(max <= value, min > value)
      default: break;
      }

      // by default - let's do row scan
      return BlockEval::PARTIAL;
    };
  }

  return uncertain;
}

// Optimization for case of "column >= C"
template <>
EvalBlock buildEvalBlock<LogicalOp::GE>(const std::unique_ptr<ValueEval>& left,
                                        const std::unique_ptr<ValueEval>& right) {
  if (left->expressionType() == ExpressionType::COLUMN
      && right->expressionType() == ExpressionType::CONSTANT) {
    // column expr signature is composed by "F:{col}"
    // const expr signature is compsoed by "C:{col}"
    std::string colName(left->signature().substr(2));
    return [name = std::move(colName), c = right.get()](const Block& b) -> BlockEval {
      EvalContext ctx{ false };
      // logic
      auto ct = b.columnType(name);
      switch (ct->k()) {
        DISPATCH_CASES(max < value, min >= value)
      default: break;
      }

      // by default - let's do row scan
      return BlockEval::PARTIAL;
    };
  }

  return uncertain;
}

// Optimization for case of "column < C"
template <>
EvalBlock buildEvalBlock<LogicalOp::LT>(const std::unique_ptr<ValueEval>& left,
                                        const std::unique_ptr<ValueEval>& right) {
  if (left->expressionType() == ExpressionType::COLUMN
      && right->expressionType() == ExpressionType::CONSTANT) {
    // column expr signature is composed by "F:{col}"
    // const expr signature is compsoed by "C:{col}"
    std::string colName(left->signature().substr(2));
    return [name = std::move(colName), c = right.get()](const Block& b) -> BlockEval {
      EvalContext ctx{ false };
      // logic
      auto ct = b.columnType(name);
      switch (ct->k()) {
        DISPATCH_CASES(min >= value, max < value)
      default: break;
      }

      // by default - let's do row scan
      return BlockEval::PARTIAL;
    };
  }

  return uncertain;
}

// Optimization for case of "column < C"
template <>
EvalBlock buildEvalBlock<LogicalOp::LE>(const std::unique_ptr<ValueEval>& left,
                                        const std::unique_ptr<ValueEval>& right) {
  if (left->expressionType() == ExpressionType::COLUMN
      && right->expressionType() == ExpressionType::CONSTANT) {
    // column expr signature is composed by "F:{col}"
    // const expr signature is compsoed by "C:{col}"
    std::string colName(left->signature().substr(2));
    return [name = std::move(colName), c = right.get()](const Block& b) -> BlockEval {
      EvalContext ctx{ false };
      // logic
      auto ct = b.columnType(name);
      switch (ct->k()) {
        DISPATCH_CASES(min > value, max <= value)
      default: break;
      }

      // by default - let's do row scan
      return BlockEval::PARTIAL;
    };
  }

  return uncertain;
}

#undef DISPATCH_CASES
#undef MIN_MAX_COMPARE

// condition "column > C", if max(column) <= C, no records match
// condition "column > C", if min(column) > C, all records match
// if the column is partition column, we use partition values, otherwise use histogram
#define CHECK_HIST(HT)                                           \
  auto histo = std::dynamic_pointer_cast<HT>(b.histogram(name)); \
  auto min = histo->min();                                       \
  auto max = histo->max();                                       \
  if (min > value || max < value) {                              \
    return N;                                                    \
  }

#define EQUAL_COMPARE(KIND, NOT)                                               \
  using ET = TypeTraits<Kind::KIND>::CppType;                                  \
  using CT = std::conditional_t<Kind::KIND == Kind::VARCHAR, std::string, ET>; \
  auto A = NOT ? BlockEval::NONE : BlockEval::ALL;                             \
  auto N = NOT ? BlockEval::ALL : BlockEval::NONE;                             \
  auto value = c->eval<ET>(ctx);                                               \
  auto values = b.partitionValues(name);                                       \
  if (values.size() > 0) {                                                     \
    for (auto v : values) {                                                    \
      auto ev = std::any_cast<CT>(v);                                          \
      if (ev == value) {                                                       \
        return values.size() == 1 ? A : BlockEval::PARTIAL;                    \
      }                                                                        \
    }                                                                          \
    return N;                                                                  \
  }                                                                            \
  if (value != std::nullopt && !b.probably(name, value.value())) {             \
    return N;                                                                  \
  }

#define DISPATCH_CASE(KIND, NOT, HT) \
  case Kind::KIND: {                 \
    EQUAL_COMPARE(KIND, NOT)         \
    CHECK_HIST(HT)                   \
    break;                           \
  }

// Optimization for case of "column == C"
template <>
EvalBlock buildEvalBlock<LogicalOp::EQ>(const std::unique_ptr<ValueEval>& left,
                                        const std::unique_ptr<ValueEval>& right) {
  if (left->expressionType() == ExpressionType::COLUMN
      && right->expressionType() == ExpressionType::CONSTANT) {
    // column expr signature is composed by "F:{col}"
    // const expr signature is compsoed by "C:{col}"
    std::string colName(left->signature().substr(2));
    return [name = std::move(colName), c = right.get()](const Block& b) -> BlockEval {
      EvalContext ctx{ false };
      // logic
      auto ct = b.columnType(name);
      switch (ct->k()) {
        DISPATCH_CASE(TINYINT, false, IntHistogram)
        DISPATCH_CASE(SMALLINT, false, IntHistogram)
        DISPATCH_CASE(INTEGER, false, IntHistogram)
        DISPATCH_CASE(BIGINT, false, IntHistogram)
        DISPATCH_CASE(REAL, false, RealHistogram)
        DISPATCH_CASE(DOUBLE, false, RealHistogram)
      case Kind::VARCHAR: {
        EQUAL_COMPARE(VARCHAR, false)
        break;
      }
      default: break;
      }

      // by default - let's do row scan
      return BlockEval::PARTIAL;
    };
  }

  return uncertain;
}

// Optimization for case of "column != C"
template <>
EvalBlock buildEvalBlock<LogicalOp::NEQ>(const std::unique_ptr<ValueEval>& left,
                                         const std::unique_ptr<ValueEval>& right) {
  if (left->expressionType() == ExpressionType::COLUMN
      && right->expressionType() == ExpressionType::CONSTANT) {
    // column expr signature is composed by "F:{col}"
    // const expr signature is compsoed by "C:{col}"
    std::string colName(left->signature().substr(2));
    return [name = std::move(colName), c = right.get()](const Block& b) -> BlockEval {
      EvalContext ctx{ false };
      // logic
      auto ct = b.columnType(name);
      switch (ct->k()) {
        DISPATCH_CASE(TINYINT, true, IntHistogram)
        DISPATCH_CASE(SMALLINT, true, IntHistogram)
        DISPATCH_CASE(INTEGER, true, IntHistogram)
        DISPATCH_CASE(BIGINT, true, IntHistogram)
        DISPATCH_CASE(REAL, true, RealHistogram)
        DISPATCH_CASE(DOUBLE, true, RealHistogram)
      case Kind::VARCHAR: {
        EQUAL_COMPARE(VARCHAR, true)
        break;
      }
      default: break;
      }

      // by default - let's do row scan
      return BlockEval::PARTIAL;
    };
  }

  return uncertain;
}

#undef DISPATCH_CASE
#undef EQUAL_COMPARE
#undef CHECK_HIST

template <BlockEval L, BlockEval R, LogicalOp O>
struct BELogic {};

#define BLOCKEVAL_COMBINE_RESULT(L, R, OP, X)                 \
  template <>                                                 \
  struct BELogic<BlockEval::L, BlockEval::R, LogicalOp::OP> { \
    static constexpr BlockEval Result = BlockEval::X;         \
  };

// define evaluation result when left and right has evaluation result
BLOCKEVAL_COMBINE_RESULT(ALL, ALL, AND, ALL)
BLOCKEVAL_COMBINE_RESULT(ALL, NONE, AND, NONE)
BLOCKEVAL_COMBINE_RESULT(ALL, PARTIAL, AND, PARTIAL)
BLOCKEVAL_COMBINE_RESULT(NONE, ALL, AND, NONE)
BLOCKEVAL_COMBINE_RESULT(NONE, NONE, AND, NONE)
BLOCKEVAL_COMBINE_RESULT(NONE, PARTIAL, AND, NONE)
BLOCKEVAL_COMBINE_RESULT(PARTIAL, ALL, AND, PARTIAL)
BLOCKEVAL_COMBINE_RESULT(PARTIAL, NONE, AND, NONE)
BLOCKEVAL_COMBINE_RESULT(PARTIAL, PARTIAL, AND, PARTIAL)

BLOCKEVAL_COMBINE_RESULT(ALL, ALL, OR, ALL)
BLOCKEVAL_COMBINE_RESULT(ALL, NONE, OR, ALL)
BLOCKEVAL_COMBINE_RESULT(ALL, PARTIAL, OR, PARTIAL)
BLOCKEVAL_COMBINE_RESULT(NONE, ALL, OR, ALL)
BLOCKEVAL_COMBINE_RESULT(NONE, NONE, OR, NONE)
BLOCKEVAL_COMBINE_RESULT(NONE, PARTIAL, OR, PARTIAL)
BLOCKEVAL_COMBINE_RESULT(PARTIAL, ALL, OR, PARTIAL)
BLOCKEVAL_COMBINE_RESULT(PARTIAL, NONE, OR, PARTIAL)
BLOCKEVAL_COMBINE_RESULT(PARTIAL, PARTIAL, OR, PARTIAL)

#undef BLOCKEVAL_COMBINE_RESULT

#define ITERATE_RIGHT(LEFT)                                                                 \
  switch (right) {                                                                          \
  case BlockEval::ALL: return BELogic<BlockEval::LEFT, BlockEval::ALL, op>::Result;         \
  case BlockEval::NONE: return BELogic<BlockEval::LEFT, BlockEval::NONE, op>::Result;       \
  case BlockEval::PARTIAL: return BELogic<BlockEval::LEFT, BlockEval::PARTIAL, op>::Result; \
  default: return BlockEval::PARTIAL;                                                       \
  }

template <LogicalOp op>
BlockEval result(BlockEval left, BlockEval right) {
  switch (left) {
  case BlockEval::ALL:
    ITERATE_RIGHT(ALL)
  case BlockEval::NONE:
    ITERATE_RIGHT(NONE)
  case BlockEval::PARTIAL:
    ITERATE_RIGHT(PARTIAL)
  default: return BlockEval::PARTIAL;
  }
}

#undef ITERATE_RIGHT

template <>
EvalBlock buildEvalBlock<LogicalOp::AND>(const std::unique_ptr<ValueEval>& left,
                                         const std::unique_ptr<ValueEval>& right) {
  return [l = left.get(), r = right.get()](const Block& b) -> BlockEval {
    auto left = l->eval(b);
    auto right = r->eval(b);
    return result<LogicalOp::AND>(left, right);
  };
}

template <>
EvalBlock buildEvalBlock<LogicalOp::OR>(const std::unique_ptr<ValueEval>& left,
                                        const std::unique_ptr<ValueEval>& right) {
  return [l = left.get(), r = right.get()](const Block& b) -> BlockEval {
    auto left = l->eval(b);
    auto right = r->eval(b);
    return result<LogicalOp::OR>(left, right);
  };
}

#undef EvalBlock

} // namespace eval
} // namespace surface
} // namespace nebula