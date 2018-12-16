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
#include <stack>
#include "Errors.h"

namespace nebula {
namespace type {

TokenType Token::lookup(const std::string& token) {
  // define all supported token keys
  static const std::unordered_map<std::string, TokenType> map = {
    { ",", TokenType::COMMA },
    { ":", TokenType::COLON },
    { "<", TokenType::OPEN_BRACKET },
    { ">", TokenType::CLOSE_BRACKET },
    { "BOOL", TokenType::TBOOLEAN },
    { "BOOLEAN", TokenType::TBOOLEAN },
    { "TINYINT", TokenType::TTINYINT },
    { "BYTE", TokenType::TTINYINT },
    { "CHAR", TokenType::TTINYINT },
    { "SMALLINT", TokenType::TSMALLINT },
    { "SHORT", TokenType::TSMALLINT },
    { "BIGINT", TokenType::TBIGINT },
    { "LONG", TokenType::TBIGINT },
    { "REAL", TokenType::TREAL },
    { "FLOAT", TokenType::TREAL },
    { "DOUBLE", TokenType::TDOUBLE },
    { "DECIMAL", TokenType::TDOUBLE },
    { "VARCHAR", TokenType::TVARCHAR },
    { "STRING", TokenType::TVARCHAR },
    { "BINARY", TokenType::TVARCHAR },
    { "ARRAY", TokenType::TARRAY },
    { "LIST", TokenType::TARRAY },
    { "VECTOR", TokenType::TARRAY },
    { "MAP", TokenType::TMAP },
    { "DICTIONARY", TokenType::TMAP },
    { "STRUCT", TokenType::TSTRUCT },
    { "ROW", TokenType::TSTRUCT }
  };

  // END stream token
  if (token.length() == 0) {
    return TokenType::END;
  }

  // do the look up and return type
  // return TINVALID if not found
  std::string key = token;
  std::transform(key.begin(), key.end(), key.begin(), ::toupper);
  auto result = map.find(key);
  if (result == map.end()) {
    return TokenType::TINVALID;
  }

  return result->second;
}

std::string Parser::nextToken() {
  auto max = stream_.length();

  // END of parsing
  if (cursor_ >= max) {
    // should return END?
    return "";
  }

  // put copy into next
  auto size = 0;
  std::stringstream ss;
  while (cursor_ < max) {
    // read a char
    char ch = stream_.at(cursor_++);

    // skip space
    if (ch == ' ') {
      continue;
    }

    // token ender
    if (ch == ':' || ch == ',' || ch == '<' || ch == '>') {
      // current token already have something, we leave this token to next
      if (size > 0) {
        cursor_--;
        return ss.str();
      }

      // otherwise, we are single letter token
      return std::string(1, ch);
    }

    ss << ch;
    size++;
  }

  THROW_RUNTIME("Next Token: should not reach here.");
}

static std::shared_ptr<Node> makeNode(std::stack<std::shared_ptr<Token>>& tokens) {
  N_ENSURE(!tokens.empty(), "tokens can't be empty");

  // I:T or T
  auto token = tokens.top();
  tokens.pop();
  N_ENSURE(token->isType(), "top has to be type token");

  // check if this type has identifier
  if (!tokens.empty()) {
    auto prev = tokens.top();
    if (prev->isColon()) {
      tokens.pop();

      // pop identifier
      auto id = tokens.top();
      tokens.pop();

      // The token name maybe treated as type if it went through lookup
      // But since it is before colon, we modify it as identifier
      // TODO(cao): Add some valid name regex rule here
      id->type = TokenType::IDENTIFIER;
      return std::make_shared<Node>(*token, id->token);
    }
  }

  return std::make_shared<Node>(*token, "");
}

static void closeNode(std::stack<std::shared_ptr<Node>>& nodes) {
  // the nodes will be C<1-2>
  std::stack<std::shared_ptr<Node>> reverse;

  auto node = nodes.top();
  nodes.pop();
  N_ENSURE(node->token.type == TokenType::CLOSE_BRACKET, "close token expected");
  while (!nodes.empty()) {
    auto x = nodes.top();
    nodes.pop();
    if (x->token.type == TokenType::OPEN_BRACKET) {
      N_ENSURE(!reverse.empty(), "compound type can't have 0 children");
      break;
    }

    reverse.push(x);
  }

  // one more before open bracket is parent node
  N_ENSURE(!nodes.empty() && !reverse.empty(), "should have node before open-bracket");
  auto parent = nodes.top();
  while (!reverse.empty()) {
    parent->addChild(std::static_pointer_cast<Tree<Node*>>(reverse.top()));
    reverse.pop();
  }
}

std::shared_ptr<Node> Parser::parse() {
  std::stack<std::shared_ptr<Node>> nodes;
  std::stack<std::shared_ptr<Token>> tokens;

  // create token
  while (true) {
    auto token = std::make_shared<Token>(nextToken());

    // end of stream
    if (token->type == TokenType::END) {
      break;
    }

    // comma token - make a node
    if (token->type == TokenType::COMMA) {
      // this is true only when last token is close-bracket
      // which already created last node and tokens may be empty
      if (!tokens.empty()) {
        nodes.push(makeNode(tokens));
      }
      continue;
    }

    // open bracket token: push a node and push open to node too
    if (token->type == TokenType::OPEN_BRACKET) {
      nodes.push(makeNode(tokens));
      nodes.push(std::make_shared<Node>(*token, "<"));
      continue;
    }

    // close bracket token: push a node and close a node
    if (token->type == TokenType::CLOSE_BRACKET) {
      nodes.push(makeNode(tokens));
      nodes.push(std::make_shared<Node>(*token, ">"));
      closeNode(nodes);
      continue;
    }

    // the rest of tokens: identifier, colon, and type
    tokens.push(token);
  }

  // tokens should be all processed
  N_ENSURE(tokens.empty(), "tokens left - check your schema");
  N_ENSURE_EQ(nodes.size(), 1);
  return nodes.top();
}

#define SCALAR_TYPE_CREATE(TT, TYPE)                                                                  \
  case TokenType::TT: {                                                                               \
    N_ENSURE(children.size() == 0, "scalar type has no children");                                    \
    return std::static_pointer_cast<TreeBase>(std::make_shared<TYPE>(TYPE::create(node.identifier))); \
  }

std::shared_ptr<RowType> TypeSerializer::from(const std::string& text) {
  Parser parser(text);
  LOG(INFO) << "Parsing schema: " << text;
  auto root = parser.parse();

  // walk this node tree before transforming it to a RowType
  auto typeTree = root->treeWalk<std::shared_ptr<TreeBase>>(
    [](const auto& v) {},
    [](const auto& v, std::vector<std::shared_ptr<TreeBase>>& children) {
      const auto& node = dynamic_cast<const Node&>(v);
      switch (node.token.type) {
        SCALAR_TYPE_CREATE(TBOOLEAN, BoolType)
        SCALAR_TYPE_CREATE(TTINYINT, ByteType)
        SCALAR_TYPE_CREATE(TSMALLINT, ShortType)
        SCALAR_TYPE_CREATE(TINTEGER, IntType)
        SCALAR_TYPE_CREATE(TBIGINT, LongType)
        SCALAR_TYPE_CREATE(TREAL, RealType)
        SCALAR_TYPE_CREATE(TDOUBLE, DoubleType)
        SCALAR_TYPE_CREATE(TVARCHAR, StringType)
      case TokenType::TARRAY: {
        N_ENSURE_EQ(children.size(), 1, "list type has single child");
        return ListType::create(node.identifier, children);
      }
      case TokenType::TMAP: {
        N_ENSURE_EQ(children.size(), 2, "map type has two children");
        return MapType::create(node.identifier, children);
      }
      case TokenType::TSTRUCT: {
        N_ENSURE_GT(children.size(), 0, "struct type has at least one child");
        return RowType::create(node.identifier, children);
      }
      default:
        throw NebulaException(fmt::format("Not supported type: {0}", node.token.token));
      }
    });

  return std::static_pointer_cast<RowType>(typeTree);
}

#undef SCALAR_TYPE_CREATE

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////== Serialization Part ==/////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
template <Kind K>
static auto convert(const Type<K>& t) -> typename std::enable_if<TypeTraits<K>::isPrimitive, std::string>::type {
  const auto& name = t.name();
  return name.length() ? fmt::format("{0}:{1}", name, t.type) : t.type;
}

#define DISPATCH_SCALAR(KIND, TYPE)                                 \
  case Kind::KIND: {                                                \
    N_ENSURE_EQ(children.size(), 0, "scalar type has no children"); \
    return convert(dynamic_cast<const TYPE&>(v));                   \
  }

std::string TypeSerializer::to(const std::shared_ptr<RowType> row) {
  // walk this tree and print all its name
  return row->treeWalk<std::string>(
    [](const auto& v) {},
    [](const auto& v, std::vector<std::string>& children) {
      auto id = v.getId();
      auto kind = static_cast<Kind>(id);
      // dispatch the handling
      switch (kind) {
        DISPATCH_SCALAR(BOOLEAN, BoolType)
        DISPATCH_SCALAR(TINYINT, ByteType)
        DISPATCH_SCALAR(SMALLINT, ShortType)
        DISPATCH_SCALAR(INTEGER, IntType)
        DISPATCH_SCALAR(BIGINT, LongType)
        DISPATCH_SCALAR(REAL, RealType)
        DISPATCH_SCALAR(DOUBLE, DoubleType)
        DISPATCH_SCALAR(VARCHAR, StringType)
      case Kind::ARRAY: {
        N_ENSURE_EQ(children.size(), 1, "list type has single child");
        const auto& list = dynamic_cast<const ListType&>(v);
        const auto& name = list.name();
        return name.length() > 0 ? fmt::format("{0}:ARRAY<{1}>", name, children[0]) : fmt::format("ARRAY<{0}>", children[0]);
      }
      case Kind::MAP: {
        N_ENSURE_EQ(children.size(), 2, "map type has two children");
        const auto& map = dynamic_cast<const MapType&>(v);
        const auto& name = map.name();
        return name.length() > 0 ? fmt::format("{0}:MAP<{1}, {2}>", name, children[0], children[1]) : fmt::format("MAP<{0}, {1}>", children[0], children[1]);
      }
      case Kind::STRUCT: {
        N_ENSURE_GT(children.size(), 0, "struct type has at least one child");
        const auto& object = dynamic_cast<const StructType&>(v);
        const auto& name = object.name();
        std::stringstream buffer;
        buffer << children[0];
        for (size_t i = 1, c = children.size(); i < c; ++i) {
          buffer << ',';
          buffer << children[i];
        }

        return name.length() > 0 ? fmt::format("{0}:STRUCT<{1}>", name, buffer.str()) : fmt::format("STRUCT<{0}>", buffer.str());
      }
      default:
        throw NebulaException(fmt::format("Not supported type: {0}", id));
      }
    });
}

#undef DISPATCH_SCALAR

} // namespace type
} // namespace nebula