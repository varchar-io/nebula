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

#include "QueryHandler.h"

#include <folly/Conv.h>
#include <gflags/gflags.h>

#include "common/Zip.h"
#include "execution/BlockManager.h"
#include "execution/core/ServerExecutor.h"
#include "service/node/NodeClient.h"
#include "service/node/RemoteNodeConnector.h"

DEFINE_uint32(AUTO_WINDOW_SIZE, 100, "maximum data point when selecting auto window");
// TODO(cao): read setting from API call rather than env setting - filter most of value=1 frames
DEFINE_uint64(TREE_PATH_MIN_SIZE, 3, "min size of tree merge path to return - should be passed from client.");

/**
 * Define some basic sharable proerpties for nebula service
 */
namespace nebula {
namespace service {
namespace server {

using nebula::api::dsl::col;
using nebula::api::dsl::count;
using nebula::api::dsl::max;
using nebula::api::dsl::min;
using nebula::api::dsl::sum;

using nebula::api::dsl::between;
using nebula::api::dsl::ColumnExpression;
using nebula::api::dsl::ConstExpression;
using nebula::api::dsl::Expression;
using nebula::api::dsl::in;
using nebula::api::dsl::like;
using nebula::api::dsl::LogicalExpression;
using nebula::api::dsl::nin;
using nebula::api::dsl::Query;
using nebula::api::dsl::reverse;
using nebula::api::dsl::SortType;
using nebula::api::dsl::starts;
using nebula::api::dsl::table;

using nebula::common::Zip;
using nebula::common::ZipFormat;
using nebula::execution::BlockManager;
using nebula::execution::Error;
using nebula::execution::PlanPtr;
using nebula::execution::QueryContext;
using nebula::execution::QueryWindow;
using nebula::execution::core::NodeConnector;
using nebula::execution::core::ServerExecutor;
using nebula::meta::NNode;
using nebula::meta::Table;
using nebula::service::Operation;
using nebula::service::base::ErrorCode;
using nebula::service::base::ServiceProperties;
using nebula::surface::EmptyRowCursor;
using nebula::surface::RowCursorPtr;
using nebula::surface::eval::LogicalOp;
using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::TypeNode;
using nebula::type::TypeTraits;

PlanPtr QueryHandler::compile(
  const std::shared_ptr<Query> query,
  const QueryWindow& window,
  std::unique_ptr<QueryContext> context,
  ErrorCode& err) const noexcept {

  // previous error blocks processing
  if (context->getError() != Error::NONE) {
    err = ErrorCode::FAIL_COMPILE_QUERY;
    return {};
  }

  // 2. build query out of the request
  PlanPtr plan = nullptr;
  auto error = Error::NONE;

  // even though compile is marked as "noexcept", there are still possible failure
  // that we should catch here to prevent crashing nebula server
  try {
    plan = query->compile(std::move(context));
    error = plan->ctx().getError();
  } catch (std::exception& ex) {
    LOG(ERROR) << "query compile error: " << ex.what();
    error = Error::INVALID_QUERY;
  }

  if (error != Error::NONE) {
    LOG(ERROR) << "Error in building query: " << (int)error;
    // translate into service error code
    switch (error) {
    case Error::NO_AUTH: {
      err = ErrorCode::AUTH_REQUIRED;
      break;
    }
    case Error::TABLE_PERM:
    case Error::COLUMN_PERM: {
      err = ErrorCode::PERMISSION_REQUIRED;
      break;
    }
    default: {
      err = ErrorCode::FAIL_COMPILE_QUERY;
      break;
    }
    }

    return {};
  }

  // set time window in query plan
  plan->setWindow(window);
  return plan;
}

RowCursorPtr QueryHandler::query(
  folly::ThreadPoolExecutor& pool,
  const PlanPtr plan,
  const std::shared_ptr<NodeConnector> connector,
  ErrorCode& err) const noexcept {
  // execute the query plan
  try {
    // create a node connector for this executor
    return ServerExecutor(NNode::local().toString()).execute(pool, plan, connector);
  } catch (const std::exception& exp) {
    LOG(ERROR) << "Error in executing query: " << exp.what();
    err = ErrorCode::FAIL_EXECUTE_QUERY;
    return EmptyRowCursor::instance();
  }
}

inline SortType orderTypeConvert(OrderType type) {
  return type == OrderType::DESC ? SortType::DESC : SortType::ASC;
}

// build the query object to execute
std::shared_ptr<Query> QueryHandler::build(const Table& tb, const QueryRequest& req, ErrorCode& err) const noexcept {
  // 1. validate the query request, if failed, we can return right away
  if ((err = validate(req)) != 0) {
    return {};
  }

  try {
    // create a node connector for this executor
    return buildQuery(tb, req);
  } catch (const std::exception& exp) {
    LOG(ERROR) << "Error in building query: " << exp.what();
    err = ErrorCode::FAIL_BUILD_QUERY;
    return {};
  }
}

std::shared_ptr<Query> QueryHandler::buildQuery(const Table& tb, const QueryRequest& req) const {
  // build filter
  auto q = std::make_shared<Query>(req.table(), ms_);

  std::shared_ptr<Expression> expr = nullptr;
#define BUILD_EXPR(PREDS, LOP)                                    \
  auto preds = req.PREDS();                                       \
  auto size = preds.expression_size();                            \
  if (size > 0) {                                                 \
    expr = buildPredicate(preds.expression(0), tb, nullptr, LOP); \
    for (auto i = 1; i < size; ++i) {                             \
      expr = buildPredicate(preds.expression(i), tb, expr, LOP);  \
    }                                                             \
  }

  switch (req.filter_case()) {
  case QueryRequest::FilterCase::kFilterA: {
    BUILD_EXPR(filtera, LogicalOp::AND)
    break;
  }
  case QueryRequest::FilterCase::kFilterO: {
    BUILD_EXPR(filtero, LogicalOp::OR)
    break;
  }
  default: break;
  }
#undef BUILD_EXPR

  // set filter - has normal combined filter or not
  using LT = TypeTraits<Kind::BIGINT>::CppType;
  auto timeFilter = between(col(Table::TIME_COLUMN), (LT)req.start(), (LT)req.end());
  if (expr == nullptr) {
    q->where(timeFilter);
  } else {
    q->where(timeFilter && expr);
  }

  // apply custom columns if any sent
  const auto ctc = [](CustomType ct) {
    switch (ct) {
    case CustomType::INT: return Kind::INTEGER;
    case CustomType::LONG: return Kind::BIGINT;
    case CustomType::FLOAT: return Kind::REAL;
    case CustomType::DOUBLE: return Kind::DOUBLE;
    case CustomType::STRING: return Kind::VARCHAR;
    default: throw NException("Custom type not supported.");
    }
  };

  for (auto i = 0, size = req.custom_size(); i < size; ++i) {
    const auto& m = req.custom(i);
    q->apply(m.column(), ctc(m.type()), m.expr());
  }

  // build all columns to be selected
  std::vector<std::shared_ptr<Expression>> fields;
  std::vector<size_t> keys;
  std::vector<std::string> columns;

  // If this is a timeline query, we only build time as dimension ignoring any any pre-set dimensions
  const auto isTimeline = req.timeline();

  // handle query schema for timeline
  if (isTimeline) {
    columns.push_back(Table::TIME_COLUMN);

    // we have minimum size of window as 1 second to be enforced
    // so if buckets is smaller than range (seconds), we use each range as
    auto range = req.end() - req.start();
    N_ENSURE_GT(range, 0, "timeline requires end time greater than start time");

    int32_t window = (int32_t)req.window();
    auto buckets = window == 0 ? FLAGS_AUTO_WINDOW_SIZE : range / window;
    if (buckets == 0 || buckets > range) {
      buckets = range;
    }

    // recalculate window based on buckets
    window = range / buckets;
    N_ENSURE_GT(window, 0, "window should be at least 1 second");

    // only one bucket?
    std::shared_ptr<Expression> windowExpr = nullptr;
    if (buckets < 2) {
      auto w = std::make_shared<ConstExpression<int64_t>>(0);
      w->as(Table::WINDOW_COLUMN);
      windowExpr = w;
    } else {
      int64_t beginTime = (int64_t)req.start();
      // divided by window to get the bucket and times window to get a time point
      auto expr = ((col(Table::TIME_COLUMN) - beginTime) / window * window).as(Table::WINDOW_COLUMN);
      windowExpr = std::make_shared<decltype(expr)>(expr);
    }

    N_ENSURE_NOT_NULL(windowExpr, "window expr should be built");
    N_ENSURE_EQ(windowExpr->alias(), Table::WINDOW_COLUMN, "expected window alias");
    fields.push_back(windowExpr);

    // timeline doesn't follow limit settings
    q->limit(0);

    keys.push_back(columns.size());
  }

  // push other dimensions
  for (auto i = 0, size = req.dimension_size(); i < size; ++i) {
    const auto& colName = req.dimension(i);
    columns.push_back(colName);
    fields.push_back(std::make_shared<ColumnExpression>(colName));

    // group by clause uses 1-based index
    keys.push_back(columns.size());
  }

  for (auto i = 0, size = req.metric_size(); i < size; ++i) {
    const auto& m = req.metric(i);
    // build metric may change column name, using its alais
    columns.push_back(m.column());
    fields.push_back(buildMetric(m, tb));
  }

  q->select(fields).groupby(keys);

  // build sorting and limit/top property
  // TODO(cao): BUG investigation - turn on sort by 1 field on samples
  // serialization will be broken, why?
  if (isTimeline) {
    // timeline always sort by time window as first column
    q->sortby({ 1 });
  } else if (req.has_order()) {
    auto order = req.order();
    // Search column index for sorting
    // Non-Science: we're ordering by first metric column
    // However, the column specified from client/UI could be duplicate
    // to some dimension column, such as (user_id, count(user_id)).
    // Since we place metrics after dimension, search from end to beginning
    // will help us find metrics column first and this is satisfying most cases.
    for (size_t i = columns.size(); i > 0; --i) {
      if (columns.at(i - 1) == order.column()) {
        q->sortby({ i }, orderTypeConvert(order.type()));
        break;
      }
    }
  }

  // set number of results to return
  if (!isTimeline) {
    auto limit = req.top();
    if (limit == 0) {
      limit = ServiceProperties::DEFAULT_TOP_SIZE;
    }
    q->limit(limit);
  }

  return q;
}

std::shared_ptr<Expression> QueryHandler::buildMetric(const Metric& metric, const Table& table) const {
  const auto& colName = metric.column();
#define BUILD_METRIC_EXP(TYPE, NAME, ...)                                                  \
  auto exp = NAME(col(colName), ##__VA_ARGS__).as(fmt::format("{0}.{1}", colName, #TYPE)); \
  return std::make_shared<decltype(exp)>(exp);

#define BUILD_METRIC_CASE(TYPE, NAME, ...)      \
  case Rollup::TYPE: {                          \
    BUILD_METRIC_EXP(TYPE, NAME, ##__VA_ARGS__) \
  }

  switch (metric.method()) {
    BUILD_METRIC_CASE(MAX, max)
    BUILD_METRIC_CASE(MIN, min)
    BUILD_METRIC_CASE(COUNT, count)
    BUILD_METRIC_CASE(SUM, sum)
    BUILD_METRIC_CASE(AVG, avg)
    BUILD_METRIC_CASE(P10, pct, 10)
    BUILD_METRIC_CASE(P25, pct, 25)
    BUILD_METRIC_CASE(P50, pct, 50)
    BUILD_METRIC_CASE(P75, pct, 75)
    BUILD_METRIC_CASE(P90, pct, 90)
    BUILD_METRIC_CASE(P99, pct, 99)
    BUILD_METRIC_CASE(P99_9, pct, 99.9)
    BUILD_METRIC_CASE(P99_99, pct, 99.99)
    BUILD_METRIC_CASE(TREEMERGE, tpm, FLAGS_TREE_PATH_MIN_SIZE)
    BUILD_METRIC_CASE(CARD_EST, card, true)
  case Rollup::HIST: {
    auto bm = BlockManager::init();
    auto schema = table.schema();
    Kind columnType = Kind::INVALID;
    // check column type and index
    auto index = schema->onChild(colName, [&columnType](const TypeNode& found) {
      columnType = found->k();
    });
    auto histogram = bm->hist(table.name(), index);
    switch (columnType) {
    case nebula::type::Kind::TINYINT:
    case nebula::type::Kind::SMALLINT:
    case nebula::type::Kind::INTEGER:
    case nebula::type::Kind::BIGINT: {
      auto histo = std::dynamic_pointer_cast<nebula::surface::eval::IntHistogram>(histogram);
      BUILD_METRIC_EXP(HIST, hist, histo->v_min, histo->v_max)
    }
    case nebula::type::Kind::REAL:
    case nebula::type::Kind::DOUBLE: {
      auto histo = std::dynamic_pointer_cast<nebula::surface::eval::RealHistogram>(histogram);
      BUILD_METRIC_EXP(HIST, hist, histo->v_min, histo->v_max)
    }
    default:
      throw NException("Histogram query unsupported for type.");
    }
  }

  default:
    throw NException("Rollup method not supported.");
  }

#undef BUILD_METRIC_CASE
#undef BUILD_METRIC_EXP
}

#define CHAIN_AND_RET                                  \
  if (prev != nullptr) {                               \
    if (op == LogicalOp::AND) {                        \
      auto chain = exp && prev;                        \
      return std::make_shared<decltype(chain)>(chain); \
    } else {                                           \
      auto chain = exp || prev;                        \
      return std::make_shared<decltype(chain)>(chain); \
    }                                                  \
  }                                                    \
  return std::make_shared<decltype(exp)>(exp);

inline Zip zip(const Predicate& pred) noexcept {
  switch (pred.zipformat()) {
  case nebula::service::ZipFormat::DELTA: return Zip(pred.zip(), ZipFormat::DELTA);
  default: return Zip{ "", ZipFormat::UNKNOWN };
  }
}

template <typename T>
auto vectorize(const Predicate& pred) -> std::vector<T> {
  // result vector
  std::vector<T> values;

  // for numbers (including bool), client may save values in n_values
  if constexpr (std::is_integral_v<T>) {
    const auto size = pred.n_value_size();
    if (size > 0) {
      values.reserve(size);
      for (auto i = 0; i < size; ++i) {
        values.push_back(static_cast<T>(pred.n_value(i)));
      }
      return values;
    }
  }

  // for floats (float/double), client may use them to in d_values
  if constexpr (std::is_floating_point_v<T>) {
    const auto size = pred.d_value_size();
    if (size > 0) {
      values.reserve(size);
      for (auto i = 0; i < size; ++i) {
        values.push_back(static_cast<T>(pred.d_value(i)));
      }
      return values;
    }
  }

  // default, all values will be converted from string vector if set
  const auto size = pred.value_size();
  values.reserve(size);
  for (auto i = 0; i < size; ++i) {
    values.push_back(folly::to<T>(pred.value(i)));
  }

  return values;
}

std::shared_ptr<Expression> QueryHandler::buildPredicate(
  const Predicate& pred,
  const Table& table,
  const std::shared_ptr<Expression> prev,
  const LogicalOp op) const {
  auto schema = table.schema();
  const auto& columnName = pred.column();
  Kind columnType = Kind::INVALID;
  // check column type
  schema->onChild(columnName, [&columnType](const TypeNode& found) {
    columnType = found->k();
  });

  N_ENSURE_NE(columnType, Kind::INVALID, "column not found");
  auto columnExpression = col(columnName);

  // convert the OPs: EQ -> IN, NEQ -> NIN
  // exclusively any type of the value list used - only one can be used at one time
  auto valueCount = std::max({ pred.value_size(),
                               pred.d_value_size(),
                               pred.n_value_size(),
                               pred.zipcount() });
  N_ENSURE_GT(valueCount, 0, "predicate requires at least one value");

  // if specified multiple values for EQ and NEQ, they should convert to
  // IN and NOT IN expression isntead
  const auto pop = pred.op();
  if (valueCount > 1) {
    // in expression
    if (pop == Operation::EQ || pop == Operation::NEQ) {
#define BUILD_IN_CLAUSE(KIND, UDF)                                                          \
  case Kind::KIND: {                                                                        \
    using ValueType = std::conditional<                                                     \
      Kind::KIND == Kind::VARCHAR,                                                          \
      std::string,                                                                          \
      typename TypeTraits<Kind::KIND>::CppType>::type;                                      \
    if constexpr (Kind::KIND == Kind::INTEGER || Kind::KIND == Kind::BIGINT) {              \
      if (pred.zipcount() > 0) {                                                            \
        auto exp = UDF<decltype(columnExpression), ValueType>(columnExpression, zip(pred)); \
        CHAIN_AND_RET                                                                       \
        break;                                                                              \
      }                                                                                     \
    }                                                                                       \
    auto exp = UDF(columnExpression, vectorize<ValueType>(pred));                           \
    CHAIN_AND_RET                                                                           \
    break;                                                                                  \
  }

#define PROCESS_UDF(func)                                        \
  switch (columnType) {                                          \
    BUILD_IN_CLAUSE(BOOLEAN, func)                               \
    BUILD_IN_CLAUSE(TINYINT, func)                               \
    BUILD_IN_CLAUSE(SMALLINT, func)                              \
    BUILD_IN_CLAUSE(INTEGER, func)                               \
    BUILD_IN_CLAUSE(BIGINT, func)                                \
    BUILD_IN_CLAUSE(REAL, func)                                  \
    BUILD_IN_CLAUSE(DOUBLE, func)                                \
    BUILD_IN_CLAUSE(VARCHAR, func)                               \
  default:                                                       \
    throw NException("Not supported column type in predicates"); \
  }

      if (pop == Operation::EQ) {
        PROCESS_UDF(in)
      } else {
        PROCESS_UDF(nin)
      }

#undef PROCESS_UDF
#undef BUILD_IN_CLAUSE
    }
  }

  std::shared_ptr<Expression> constExpression = nullptr;

#define BUILD_CONST_CASE(KIND)                                                                   \
  case Kind::KIND: {                                                                             \
    using T = TypeTraits<Kind::KIND>::CppType;                                                   \
    if constexpr (std::is_integral_v<T>) {                                                       \
      if (pred.n_value_size() > 0) {                                                             \
        constExpression = std::make_shared<ConstExpression<T>>(static_cast<T>(pred.n_value(0))); \
        break;                                                                                   \
      }                                                                                          \
    }                                                                                            \
    if constexpr (std::is_floating_point_v<T>) {                                                 \
      if (pred.d_value_size() > 0) {                                                             \
        constExpression = std::make_shared<ConstExpression<T>>(static_cast<T>(pred.d_value(0))); \
        break;                                                                                   \
      }                                                                                          \
    }                                                                                            \
    constExpression = std::make_shared<ConstExpression<T>>(folly::to<T>(pred.value(0)));         \
    break;                                                                                       \
  }

  // note: we convert the constant expression to the column type so that they align
  // in the evaluation phase, we will use column type to decide how to eval constant value
  switch (columnType) {
    BUILD_CONST_CASE(BOOLEAN)
    BUILD_CONST_CASE(TINYINT)
    BUILD_CONST_CASE(SMALLINT)
    BUILD_CONST_CASE(INTEGER)
    BUILD_CONST_CASE(BIGINT)
    BUILD_CONST_CASE(REAL)
    BUILD_CONST_CASE(DOUBLE)
  case Kind::VARCHAR: {
    constExpression = std::make_shared<ConstExpression<std::string>>(pred.value(0));
    break;
  }
  default:
    throw NException("Not supported column type in predicates");
  }
#undef BUILD_CONST_CASE

  N_ENSURE(op == LogicalOp::AND || op == LogicalOp::OR, "connection op can only be AND or OR");

// build the logical expression based on op
#define BUILD_LOGICAL_CASE(TYPE, OP)                \
  case Operation::TYPE: {                           \
    auto exp = columnExpression OP constExpression; \
    CHAIN_AND_RET                                   \
  }

// Optimization:
// like expression can be fall back to "starts" if the pattern satisfy it
// REV is to wrap `not()` around the expression
#define BUILD_LIKE(CS, REV)                                               \
  static constexpr char matcher = '%';                                    \
  const auto& pattern = pred.value(0);                                    \
  size_t pos = 0;                                                         \
  auto cursor = pattern.cbegin();                                         \
  auto end = pattern.cend();                                              \
  while (cursor < end) {                                                  \
    if (*cursor == matcher) {                                             \
      break;                                                              \
    }                                                                     \
                                                                          \
    ++pos;                                                                \
    ++cursor;                                                             \
  }                                                                       \
                                                                          \
  if (pos == pattern.size() - 1) {                                        \
    auto exp = starts(columnExpression, pattern.substr(0, pos), CS, REV); \
    CHAIN_AND_RET                                                         \
  }                                                                       \
                                                                          \
  auto exp = like(columnExpression, pattern, CS, REV);                    \
  CHAIN_AND_RET

  switch (pop) {
    BUILD_LOGICAL_CASE(EQ, ==)
    BUILD_LOGICAL_CASE(NEQ, !=)
    BUILD_LOGICAL_CASE(MORE, >)
    BUILD_LOGICAL_CASE(LESS, <)
  case Operation::LIKE: {
    BUILD_LIKE(true, false)
  }
  case Operation::ILIKE: {
    BUILD_LIKE(false, false)
  }
  case Operation::UNLIKE: {
    BUILD_LIKE(true, true)
  }
  case Operation::IUNLIKE: {
    BUILD_LIKE(false, true)
  }
  default:
    throw NException("Nebula predicate operation not supported yet");
  }

#undef BUILD_LIKE
#undef BUILD_LOGICAL_CASE
}

#undef CHAIN_AND_RET

// validate the query request
ErrorCode QueryHandler::validate(const QueryRequest& req) const noexcept {
  if (req.table() == "") {
    return ErrorCode::MISSING_TABLE;
  }

  if (req.start() == 0 || req.end() == 0) {
    return ErrorCode::MISSING_TIME_RANGE;
  }

  // at least has more than 1 column (dimensions or metrics)
  if (req.dimension_size() + req.metric_size() == 0) {
    return ErrorCode::MISSING_OUTPUT_FIELDS;
  }

  // pass validation
  return ErrorCode::NONE;
}

} // namespace server
} // namespace service
} // namespace nebula