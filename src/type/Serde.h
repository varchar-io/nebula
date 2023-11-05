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

#pragma once

#include <stack>

#include "Type.h"
#include "common/Params.h"

namespace nebula {
namespace type {

// key-value settings in both string types
using Settings = nebula::common::MapKV;

/*
 *
 * A type tree serializer
 * Here we use default string serialization format which is compliant with hive
 * This can be override with other format such as JSON/protobuf/thrift
 *
 */
class TypeSerializer {
public:
  // deserialize or parse the type tree from given string
  static std::shared_ptr<RowType> from(const std::string& text);

  // serialize or persist the type tree as readable string
  static std::string to(const std::shared_ptr<RowType> row);
};

enum class TokenType {
  // separate field by ','
  COMMA = 0,
  // separate identifier and type by ':'
  COLON = 1,
  // define contianer of compound '<' '>'
  OPEN_BRACKET = 2,
  CLOSE_BRACKET = 3,

  // any identifier that compliant with name convention
  IDENTIFIER = 4,

  // all types - ref TypeKind definition
  TINVALID = 5,
  TBOOLEAN = 6,
  TTINYINT = 7,
  TSMALLINT = 8,
  TINTEGER = 9,
  TBIGINT = 10,
  TREAL = 11,
  TDOUBLE = 12,
  TVARCHAR = 13,
  TARRAY = 14,
  TMAP = 15,
  TSTRUCT = 16,
  TINT128 = 17,

  // define a END token
  END = 99
};

struct Token {
  static TokenType lookup(const std::string&);
  explicit Token(const std::string& t) : token{ t }, type{ lookup(t) } {}

  std::string token;
  TokenType type;

  bool isType() const {
    return type > TokenType::TINVALID && type < TokenType::END;
  }

  bool isColon() const {
    return type == TokenType::COLON;
  }

  bool isIdentifier() const {
    return type == TokenType::IDENTIFIER || type == TokenType::TINVALID;
  }
};

// define any node data
struct Node : public Tree<Node*> {
  explicit Node(const Token& t, const std::string& i) : Tree<Node*>(this), token{ t }, identifier{ i } {}

  // recognized token type
  Token token;
  std::string identifier;

  size_t uniqueId;
  size_t id() const { return uniqueId; }

  void validate() const {
    // TODO(cao): validate current token state
  }
};

// A parser initialized with the input stream
// and has parsing state
class Parser {
public:
  Parser(const std::string& stream) : cursor_{ 0 }, stream_{ stream } {}
  virtual ~Parser() = default;

  // do the parsing and return parsed root node
  std::shared_ptr<Node> parse();

  // a throwable wrapper to make a node
  std::shared_ptr<Node> makeNode(std::stack<std::shared_ptr<Token>>&);

private:
  size_t cursor_;
  const std::string& stream_;

  // move cursor and return current token
  // ensure returned token is upper case only
  std::string nextToken();
};
} // namespace type
} // namespace nebula