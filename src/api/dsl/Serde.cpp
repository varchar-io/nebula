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

#include "Serde.h"

#include <msgpack.hpp>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include "Expressions.h"
#include "type/Type.h"

namespace nebula {
namespace api {
namespace dsl {

using nebula::common::Zip;
using nebula::common::ZipFormat;
using nebula::surface::eval::UDFType;
using nebula::type::TypeDetect;

// helper method to add a json string field
inline void addstring(rapidjson::Writer<rapidjson::StringBuffer>& json,
                      const char* const key,
                      const std::string& str) noexcept {
  json.Key(key);
  // JSON string will remove the trailing 0s, to be preserved for serde purpose
  json.String(str.data(), str.size());
}

// helper method to alias it
template <typename T>
inline std::shared_ptr<T> as(const std::string& alias, std::shared_ptr<T> ptr) {
  ptr->as(alias);
  return ptr;
}

std::string ser(const ExpressionData& data) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> json(buffer);
  json.StartObject();
  json.Key("type");
  json.Int(data.type);
  addstring(json, "alias", data.alias);

  // serialize it into json
  switch (data.type) {
  case ExpressionType::CONSTANT: {
    addstring(json, "c_type", data.c_type);
    addstring(json, "c_value", data.c_value);
    break;
  }
  // it reuses the same fields as constant expression
  case ExpressionType::SCRIPT: {
    addstring(json, "c_type", data.c_type);
    addstring(json, "c_value", data.c_value);
    break;
  }
  case ExpressionType::COLUMN: {
    addstring(json, "c_name", data.c_name);
    break;
  }
  case ExpressionType::LOGICAL: {
    json.Key("op");
    json.Int(static_cast<int>(data.b_lop));

    addstring(json, "left", ser(*data.b_left));
    addstring(json, "right", ser(*data.b_right));
    break;
  }

  case ExpressionType::ARTHMETIC: {
    json.Key("op");
    json.Int(static_cast<int>(data.b_aop));

    addstring(json, "left", ser(*data.b_left));
    addstring(json, "right", ser(*data.b_right));
    break;
  }
  case ExpressionType::FUNCTION: {
    json.Key("udf");
    json.Int(static_cast<int>(data.u_type));
    addstring(json, "inner", ser(*data.inner));
    // append an tail to keep any trailing 0 in custom
    // remove tail N before consuming it
    addstring(json, "custom", data.custom);
    json.Key("flag");
    json.Bool(data.flag);
    addstring(json, "c_type", data.c_type);
    break;
  }

  default:
    throw NException("Not recognized expression!");
  }

  // wrap up json collection and return the result
  json.EndObject();
  return buffer.GetString();
}

std::string Serde::serialize(const std::vector<CustomColumn>& customs) {
  std::stringstream buffer;
  msgpack::pack(buffer, customs);
  buffer.seekg(0);
  return buffer.str();
}

std::vector<CustomColumn> Serde::deserialize(const char* data, size_t size) {
  msgpack::object_handle oh = msgpack::unpack(data, size);
  return oh.get().as<std::vector<CustomColumn>>();
}

std::string Serde::serialize(const Expression& expr) {
  auto ptr = expr.serialize();
  return ser(*ptr);
}

#define TYPED_CONST(T)                                                             \
  if (type == TypeDetect<T>::tid()) {                                              \
    using ST = TypeDetect<T>::StandardType;                                        \
    return as(alias, std::make_shared<ConstExpression<ST>>(folly::to<ST>(value))); \
  }

#define STRING_CONST(T)                                                      \
  if (type == TypeDetect<T>::tid()) {                                        \
    return as(alias, std::make_shared<ConstExpression<std::string>>(value)); \
  }

std::shared_ptr<Expression> c_expr(
  const std::string& alias,
  const std::string& type,
  const std::string& value) {
  TYPED_CONST(bool)
  TYPED_CONST(int8_t)
  TYPED_CONST(int16_t)
  TYPED_CONST(int32_t)
  TYPED_CONST(int64_t)
  TYPED_CONST(float)
  TYPED_CONST(double)
  STRING_CONST(const char*)
  STRING_CONST(char*)
  STRING_CONST(std::string_view)
  STRING_CONST(std::string)

  throw NException(fmt::format("Not recognized type: {0}", type));
}

#undef STRING_CONST
#undef TYPED_CONST

#define TYPE_SCRIPT(T)                                          \
  if (type == TypeDetect<T>::tid()) {                           \
    return std::make_shared<ScriptExpression<T>>(alias, value); \
  }

std::shared_ptr<Expression> s_expr(
  const std::string& alias,
  const std::string& type,
  const std::string& value) {
  TYPE_SCRIPT(bool)
  TYPE_SCRIPT(int8_t)
  TYPE_SCRIPT(int16_t)
  TYPE_SCRIPT(int32_t)
  TYPE_SCRIPT(int64_t)
  TYPE_SCRIPT(float)
  TYPE_SCRIPT(double)
  TYPE_SCRIPT(const char*)
  TYPE_SCRIPT(char*)
  TYPE_SCRIPT(std::string_view)
  TYPE_SCRIPT(std::string)

  throw NException(fmt::format("Not recognized type: {0}", type));
}

#undef TYEP_SCRIPT

#define LOGIC_FORM(OP)                                                                                         \
  case LogicalOp::OP: {                                                                                        \
    return as(alias, std::make_shared<LogicalExpression<LogicalOp::OP, Expression, Expression>>(left, right)); \
  }

std::shared_ptr<Expression> l_expr(const std::string& alias,
                                   LogicalOp op,
                                   std::shared_ptr<Expression> left,
                                   std::shared_ptr<Expression> right) {
  switch (op) {
    LOGIC_FORM(EQ)
    LOGIC_FORM(NEQ)
    LOGIC_FORM(GT)
    LOGIC_FORM(GE)
    LOGIC_FORM(LT)
    LOGIC_FORM(LE)
    LOGIC_FORM(AND)
    LOGIC_FORM(OR)
  default:
    throw NException(fmt::format("Unknown logical op: {0}", static_cast<int>(op)));
  }
}

#undef LOGIC_FORM

#define ARTH_FORM(OP)                                                                                              \
  case ArthmeticOp::OP: {                                                                                          \
    return as(alias, std::make_shared<ArthmeticExpression<ArthmeticOp::OP, Expression, Expression>>(left, right)); \
  }

std::shared_ptr<Expression> a_expr(const std::string& alias,
                                   ArthmeticOp op,
                                   std::shared_ptr<Expression> left,
                                   std::shared_ptr<Expression> right) {
  switch (op) {
    ARTH_FORM(ADD)
    ARTH_FORM(SUB)
    ARTH_FORM(MUL)
    ARTH_FORM(DIV)
    ARTH_FORM(MOD)
  default:
    throw NException(fmt::format("Unknown arthmetic op: {0}", static_cast<int>(op)));
  }
}

#undef ARTH_FORM

std::shared_ptr<Expression> u_expr(const std::string& alias,
                                   UDFType ut,
                                   std::shared_ptr<Expression> inner,
                                   const std::string& custom,
                                   bool flag,
                                   const std::string& input_type) {
  // like, prefix
  switch (ut) {
  case UDFType::LIKE: {
    return as(alias, std::make_shared<LikeExpression>(inner, custom, flag));
  }

  case UDFType::PREFIX: {
    return as(alias, std::make_shared<PrefixExpression>(inner, custom, flag));
  }

  case UDFType::IN: {
#define TYPE_IN_EXPR(T, F)                                                              \
  if (valueType == TypeDetect<T>::tid()) {                                              \
    if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {           \
      if (zip.format() != ZipFormat::UNKNOWN) {                                         \
        return as(alias, std::make_shared<InExpression<T>>(inner, std::move(zip), in)); \
      }                                                                                 \
    }                                                                                   \
    std::vector<T> values;                                                              \
    values.reserve(size);                                                               \
    for (rapidjson::SizeType i = 0; i < size; ++i) {                                    \
      values.push_back(v[i].F());                                                       \
    }                                                                                   \
    return as(alias, std::make_shared<InExpression<T>>(inner, std::move(values), in));  \
  }

    rapidjson::Document cd;
    if (cd.Parse(custom.c_str()).HasParseError()) {
      throw NException(fmt::format("Error parsing UDF custom data: {0}", custom));
    }

    bool in = cd["in"].GetBool();
    const auto valueType = cd["dtype"].GetString();
    Zip zip{ cd["zip-data"].GetString(), (ZipFormat)cd["zip-format"].GetInt() };
    const rapidjson::Value& v = cd["values"];
    rapidjson::SizeType size = v.Size();

    // reference InExpression serialize method
    TYPE_IN_EXPR(bool, GetBool)
    TYPE_IN_EXPR(int8_t, GetInt)
    TYPE_IN_EXPR(int16_t, GetInt)
    TYPE_IN_EXPR(int32_t, GetInt)
    TYPE_IN_EXPR(int64_t, GetInt64)
    TYPE_IN_EXPR(float, GetFloat)
    TYPE_IN_EXPR(double, GetDouble)
    TYPE_IN_EXPR(std::string, GetString)

    throw NException(fmt::format("Unrecognized value type: {0}", valueType));
#undef TYPE_IN_EXPR
  }
  case UDFType::PCT: {
    msgpack::object_handle oh = msgpack::unpack(custom.data(), custom.size());
    auto deser = oh.get();
    auto dst = deser.as<std::tuple<double>>();
    auto pct = std::get<0>(dst);
    return as(alias, std::make_shared<UDFExpression<UDFType::PCT, double>>(inner, pct));
  }
  case UDFType::CARD: {
    msgpack::object_handle oh = msgpack::unpack(custom.data(), custom.size());
    auto deser = oh.get();
    auto dst = deser.as<std::tuple<bool>>();
    auto est = std::get<0>(dst);
    return as(alias, std::make_shared<UDFExpression<UDFType::CARD, bool>>(inner, est));
  }
  case UDFType::BETWEEN: {
    msgpack::object_handle oh = msgpack::unpack(custom.data(), custom.size());
    auto deser = oh.get();
    auto typeMinMax = deser.as<std::tuple<std::string, std::string, std::string>>();
    auto tid = std::get<0>(typeMinMax);
    auto min = std::get<1>(typeMinMax);
    auto max = std::get<2>(typeMinMax);

#define TYPED_EXPR(T)                                                                                      \
  if (tid == TypeDetect<T>::tid()) {                                                                       \
    return as(alias, std::make_shared<BetweenExpression<T>>(inner, folly::to<T>(min), folly::to<T>(max))); \
  }

    TYPED_EXPR(int8_t)
    TYPED_EXPR(int16_t)
    TYPED_EXPR(int32_t)
    TYPED_EXPR(int64_t)
    TYPED_EXPR(float)
    TYPED_EXPR(double)

    throw NException(fmt::format("Unsupported data type in between: {0}", tid));
#undef TYPED_EXPR
  }
  case UDFType::HIST: {
#define TYPE_UDF_INPUT(T)                                                                                  \
  if (input_type == TypeDetect<T>::tid()) {                                                                \
    auto dst = deser.as<std::tuple<T, T>>();                                                               \
    auto min = std::get<0>(dst);                                                                           \
    auto max = std::get<1>(dst);                                                                           \
    return as(alias, std::make_shared<UDFExpression<UDFType::HIST, T, T>>(inner, min, max));               \
  }
    msgpack::object_handle oh = msgpack::unpack(custom.data(), custom.size());
    auto deser = oh.get();
    TYPE_UDF_INPUT(int8_t)
    TYPE_UDF_INPUT(int16_t)
    TYPE_UDF_INPUT(int32_t)
    TYPE_UDF_INPUT(int64_t)
    TYPE_UDF_INPUT(float)
    TYPE_UDF_INPUT(double)

    throw NException(fmt::format("Not recognized type for hist udf: {0}", input_type));
#undef TYPE_UDF_INPUT
  }

#define COM_UDF(UT)                                                        \
  case UDFType::UT: {                                                      \
    return as(alias, std::make_shared<UDFExpression<UDFType::UT>>(inner)); \
  }

    COM_UDF(NOT)
    COM_UDF(AVG)
    COM_UDF(MAX)
    COM_UDF(MIN)
    COM_UDF(COUNT)
    COM_UDF(SUM)
    COM_UDF(TPM)

#undef COM_UDF

  default:
    throw NException(fmt::format("Unrecognized UDF type: {0}", static_cast<int>(ut)));
  }
}

std::shared_ptr<Expression> Serde::deserialize(const std::string& data) {
  rapidjson::Document document;

  if (document.Parse(data.c_str()).HasParseError()) {
    throw NException(fmt::format("Invaid expression serialization data: {0}", data));
  }

  // fetching data from document
  ExpressionType type = static_cast<ExpressionType>(document["type"].GetInt());
  const auto alias = document["alias"].GetString();
  switch (type) {
  case ExpressionType::CONSTANT: {
    return c_expr(alias, document["c_type"].GetString(), document["c_value"].GetString());
  }
  case ExpressionType::SCRIPT: {
    return s_expr(alias, document["c_type"].GetString(), document["c_value"].GetString());
  }
  case ExpressionType::COLUMN: {
    return as(alias, std::make_shared<ColumnExpression>(document["c_name"].GetString()));
  }
  case ExpressionType::LOGICAL: {
    return l_expr(alias,
                  static_cast<LogicalOp>(document["op"].GetInt()),
                  deserialize(document["left"].GetString()),
                  deserialize(document["right"].GetString()));
  }
  case ExpressionType::ARTHMETIC: {
    return a_expr(alias,
                  static_cast<ArthmeticOp>(document["op"].GetInt()),
                  deserialize(document["left"].GetString()),
                  deserialize(document["right"].GetString()));
  }
  case ExpressionType::FUNCTION: {
    const auto& c = document["custom"];
    const auto& input_type = document["c_type"];
    return u_expr(alias,
                  static_cast<UDFType>(document["udf"].GetInt()),
                  deserialize(document["inner"].GetString()),
                  std::string(c.GetString(), c.GetStringLength()),
                  document["flag"].GetBool(),
                  input_type.GetString());
  }
  default:
    throw NException("Not recognized expression!");
  }
}

} // namespace dsl
} // namespace api
} // namespace nebula