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

using nebula::surface::eval::UDFType;
using nebula::type::TypeDetect;

// helper method to add a json string field
inline void addstring(rapidjson::Writer<rapidjson::StringBuffer>& json, const char* const key, const std::string& str) noexcept {
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
    break;
  }

  default:
    throw NException("Not recognized expression!");
  }

  // wrap up json collection and return the result
  json.EndObject();
  return buffer.GetString();
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

#define LOGIC_FORM(OP)                                                                                         \
  case LogicalOp::OP: {                                                                                        \
    return as(alias, std::make_shared<LogicalExpression<LogicalOp::OP, Expression, Expression>>(left, right)); \
  }

std::shared_ptr<Expression> l_expr(const std::string& alias, LogicalOp op, std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) {
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

std::shared_ptr<Expression> a_expr(const std::string& alias, ArthmeticOp op, std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) {
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

std::shared_ptr<Expression> u_expr(const std::string& alias, UDFType ut, std::shared_ptr<Expression> inner, const std::string& custom, bool flag) {
  // like, prefix
  switch (ut) {
  case UDFType::LIKE: {
    return as(alias, std::make_shared<LikeExpression>(inner, custom, flag));
  }

  case UDFType::PREFIX: {
    return as(alias, std::make_shared<PrefixExpression>(inner, custom, flag));
  }

  case UDFType::IN: {
#define TYPE_IN_EXPR(T, F)                                                  \
  if (valueType == TypeDetect<T>::tid()) {                                  \
    std::vector<T> values;                                                  \
    values.reserve(size);                                                   \
    for (rapidjson::SizeType i = 0; i < size; ++i) {                        \
      values.push_back(v[i].F());                                           \
    }                                                                       \
    return as(alias, std::make_shared<InExpression<T>>(inner, values, in)); \
  }

    rapidjson::Document cd;
    if (cd.Parse(custom.c_str()).HasParseError()) {
      throw NException(fmt::format("Error parsing UDF custom data: {0}", custom));
    }

    bool in = cd["in"].GetBool();
    const auto valueType = cd["dtype"].GetString();
    const rapidjson::Value& v = cd["values"];
    rapidjson::SizeType size = v.Size();

    // reference InExpression serialize method
    TYPE_IN_EXPR(bool, GetBool)
    TYPE_IN_EXPR(int8_t, GetInt)
    TYPE_IN_EXPR(int16_t, GetInt)
    TYPE_IN_EXPR(int32_t, GetInt)
    TYPE_IN_EXPR(float, GetFloat)
    TYPE_IN_EXPR(double, GetDouble)
    TYPE_IN_EXPR(std::string, GetString)

    // int64 use string as serialization format due to JSON limit on long type numbers
    if (valueType == TypeDetect<int64_t>::tid()) {
      std::vector<int64_t> values;
      values.reserve(size);
      for (rapidjson::SizeType i = 0; i < size; ++i) {
        values.push_back(folly::to<int64_t>(v[i].GetString()));
      }

      return as(alias, std::make_shared<InExpression<int64_t>>(inner, values, in));
    }

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
    return u_expr(alias,
                  static_cast<UDFType>(document["udf"].GetInt()),
                  deserialize(document["inner"].GetString()),
                  std::string(c.GetString(), c.GetStringLength()),
                  document["flag"].GetBool());
  }
  default:
    throw NException("Not recognized expression!");
  }
}

} // namespace dsl
} // namespace api
} // namespace nebula