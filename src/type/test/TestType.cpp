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

#include "gtest/gtest.h"
#include <any>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include "Errors.h"
#include "Serde.h"
#include "Tree.h"
#include "Type.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace nebula {
namespace type {
namespace test {
using namespace nebula::common;

TEST(TypeTest, Dummy) {
  EXPECT_EQ(4, 2 + 2);
  EXPECT_EQ(fmt::format("a{}", 1), "a1");
  N_ENSURE_EQ(4, 2 + 2);

  LOG(INFO) << fmt::format("The date is {}", 9);

  for (auto i = 0; i < 10; ++i) {
    LOG(INFO) << "COUNTING: " << i;
  }
}

TEST(TreeTest, Basic) {
  Tree<int> tree(0);
  EXPECT_EQ(tree.size(), 0);

  const auto& child = tree.addChild(std::make_shared<Tree<int>>(3));
  EXPECT_EQ(tree.size(), 1);
  EXPECT_EQ(child.size(), 0);

  const auto value = tree.childAt<int>(0).value();
  EXPECT_EQ(value, 3);

  LOG(INFO) << "ROOT: " << tree.value();
  LOG(INFO) << "CHILD: " << child.value();
}

TEST(TypeTest, Traits) {
  EXPECT_EQ(TypeTraits<Kind::BOOLEAN>::typeKind, Kind::BOOLEAN);
  EXPECT_EQ(TypeTraits<Kind::BOOLEAN>::isPrimitive, true);
  EXPECT_EQ(TypeTraits<Kind::BOOLEAN>::width, 1);

  // single type as small int
  {
    auto type = ShortType::create("x");
    EXPECT_EQ(type.kind, Kind::SMALLINT);
    EXPECT_EQ(type.isPrimitive, true);
    EXPECT_EQ(type.isFixedWidth, true);
    EXPECT_EQ(type.width, 2);
    EXPECT_EQ(type.name(), "x");
  }

  // single type as double
  {
    auto type = DoubleType::create("y");
    EXPECT_EQ(type.kind, Kind::DOUBLE);
    EXPECT_EQ(type.isPrimitive, true);
    EXPECT_EQ(type.isFixedWidth, true);
    EXPECT_EQ(type.width, 8);
    EXPECT_EQ(type.type, "DOUBLE");
  }

  // single type as string
  {
    auto type = StringType::create("z");
    EXPECT_EQ(type.kind, Kind::VARCHAR);
    EXPECT_EQ(type.isPrimitive, true);
    EXPECT_EQ(type.isFixedWidth, false);
    EXPECT_EQ(type.width, 0);
    EXPECT_EQ(type.type, "VARCHAR");
  }

  // type of list
  {
    // NOTE: create returns copy-elision object which will expire out of the scope
    // in production code, we should use shared_ptr<StringType>(new Object), API refactor?
    auto itemType = std::make_shared<StringType>(StringType::create("item"));
    auto type = ListType::create("list", itemType);
    EXPECT_EQ(type.kind, Kind::ARRAY);
    EXPECT_EQ(type.isPrimitive, false);
    EXPECT_EQ(type.isFixedWidth, false);
    EXPECT_EQ(type.width, 0);
    EXPECT_EQ(type.type, "ARRAY");
    EXPECT_EQ(type.name(), "list");
    EXPECT_EQ(type.size(), 1);
  }

  // type of map
  {
    auto keyType = std::make_shared<LongType>(LongType::create("key"));
    auto valueType = std::make_shared<StringType>(StringType::create("value"));
    auto type = MapType::create("map", keyType, valueType);

    EXPECT_EQ(type.kind, Kind::MAP);
    EXPECT_EQ(type.isPrimitive, false);
    EXPECT_EQ(type.isFixedWidth, false);
    EXPECT_EQ(type.width, 0);
    EXPECT_EQ(type.type, "MAP");
    EXPECT_EQ(type.name(), "map");
    EXPECT_EQ(type.size(), 2);
  }

  // type of struct with 2 fields
  {
    auto f1Type = std::make_shared<LongType>(LongType::create("f1"));
    auto f2Type = std::make_shared<StringType>(StringType::create("f2"));
    auto type = RowType::create("row", f1Type, f2Type);

    EXPECT_EQ(type.kind, Kind::STRUCT);
    EXPECT_EQ(type.isPrimitive, false);
    EXPECT_EQ(type.isFixedWidth, false);
    EXPECT_EQ(type.width, 0);
    EXPECT_EQ(type.type, "STRUCT");
    EXPECT_EQ(type.name(), "row");
    EXPECT_EQ(type.size(), 2);
  }

  // type of struct with 2 fields
  {
    auto type = RowType::create("row",
                                std::make_shared<LongType>(LongType::create("f1")),
                                std::make_shared<StringType>(StringType::create("f2")),
                                std::make_shared<ShortType>(ShortType::create("f3")),
                                std::make_shared<BoolType>(BoolType::create("f4")));

    EXPECT_EQ(type.size(), 4);
    auto f1 = type.childAt<LongType::PType>(0).value();
    EXPECT_EQ(f1->name(), "f1");
    EXPECT_EQ(f1->type, "BIGINT");

    auto f2 = type.childAt<StringType::PType>(1).value();
    EXPECT_EQ(f2->name(), "f2");
    EXPECT_EQ(f2->type, "VARCHAR");

    auto f3 = type.childAt<ShortType::PType>(2).value();
    EXPECT_EQ(f3->name(), "f3");
    EXPECT_EQ(f3->type, "SMALLINT");

    auto f4 = type.childAt<BoolType::PType>(3).value();
    EXPECT_EQ(f4->name(), "f4");
    EXPECT_EQ(f4->type, "BOOLEAN");
  }
}

TEST(TypeTest, TestSerde) {
  {
    auto type = RowType::create("",
                                std::make_shared<LongType>(LongType::create("f1")),
                                std::make_shared<StringType>(StringType::create("f2")),
                                std::make_shared<ShortType>(ShortType::create("f3")),
                                std::make_shared<BoolType>(BoolType::create("f4")));
    auto schema = TypeSerializer::to(std::make_shared<RowType>(type));
    EXPECT_EQ(schema, "STRUCT<f1:BIGINT,f2:VARCHAR,f3:SMALLINT,f4:BOOLEAN>");
  }

  {
    auto keyType = std::make_shared<LongType>(LongType::create("key"));
    auto valueType = std::make_shared<StringType>(StringType::create("value"));

    auto itemType = std::make_shared<StringType>(StringType::create("item"));

    auto type = RowType::create("",
                                std::make_shared<LongType>(LongType::create("f1")),
                                std::make_shared<MapType>(MapType::create("map", keyType, valueType)),
                                std::make_shared<ListType>(ListType::create("list", itemType)),
                                std::make_shared<BoolType>(BoolType::create("f4")));

    auto expected = "STRUCT<f1:BIGINT,map:MAP<key:BIGINT, value:VARCHAR>,list:ARRAY<item:VARCHAR>,f4:BOOLEAN>";
    auto schema = TypeSerializer::to(std::make_shared<RowType>(type));

    EXPECT_EQ(schema, expected);
  }

  // test deserialization
  {
    auto schema = "STRUCT<f1:BIGINT,map:MAP<key:BIGINT, value:VARCHAR>,list:ARRAY<item:VARCHAR>,f4:BOOLEAN>";
    auto type = TypeSerializer::from(schema);

    EXPECT_EQ(type->size(), 4);
    auto f1 = type->childAt<LongType::PType>(0).value();
    EXPECT_EQ(f1->name(), "f1");
    EXPECT_EQ(f1->type, "BIGINT");

    LOG(INFO) << "get map type";
    MapType::PType map = type->childAt<MapType::PType>(1).value();
    EXPECT_EQ(map->type, "MAP");
    LOG(INFO) << "passed type check";
    EXPECT_EQ(map->name(), "map");
  }
}

TEST(TypeTest, TestSerdeRoundTrip) {
  // test deserialization
  {
    auto schema = "STRUCT<f1:BIGINT,map:MAP<key:BIGINT, value:VARCHAR>,list:ARRAY<item:VARCHAR>,f4:BOOLEAN>";
    auto type = TypeSerializer::from(schema);
    LOG(INFO) << "Deserialized a type tree with number of columns: " << type->size();
    auto serialized = TypeSerializer::to(type);
    LOG(INFO) << "Serialize the tree to schema: " << serialized;

    EXPECT_EQ(schema, serialized);
  }

  // test deserialization
  {
    auto schema = "ROW:STRUCT<id:int, map<key:long, value:string>, items:list<string>, flag:bool>";
    auto expected = "ROW:STRUCT<id:INTEGER,MAP<key:BIGINT, value:VARCHAR>,items:ARRAY<VARCHAR>,flag:BOOLEAN>";
    auto type = TypeSerializer::from(schema);
    LOG(INFO) << "Deserialized a type tree with number of columns: " << type->size();
    // assign node ID to the schema
    LOG(INFO) << "Total Nodes: " << type->assignNodeId(0);
    type->treeWalk<size_t>([](const auto& v) { LOG(INFO) << "NODE: n=" << v.getNode() << ",id=" << v.getId(); }, {});
    auto serialized = TypeSerializer::to(type);
    LOG(INFO) << "Serialize the tree to schema: " << serialized;

    EXPECT_EQ(expected, serialized);
  }
}

TEST(VectorTest, CxxVersion) {
  std::vector<std::any> vec;
  vec.push_back(1);
  vec.push_back("something");
  EXPECT_EQ(vec.size(), 2);
}

/*
* To build a heterogenous container, we have choices
* - static fixed size: 
* -- std::tuple
* -- variadic template parameter pack
* - dynamic sized: 
* -- std::any of an existing contianer such as vector (any_cast)
* -- std::variant with known type set std::variant<T1, T2...>
* this test is on tuple - the pattern comes from cpppatterns.com
*/
template <typename F, typename Tuple, size_t... S>
decltype(auto) apply_tuple_impl(F&& fn, Tuple&& t, std::index_sequence<S...>) {
  return std::forward<F>(fn)(std::get<S>(std::forward<Tuple>(t))...);
}

template <typename F, typename Tuple>
decltype(auto) apply_from_tuple(F&& fn, Tuple&& t) {
  std::size_t constexpr tSize = std::tuple_size<typename std::remove_reference<Tuple>::type>::value;
  return apply_tuple_impl(std::forward<F>(fn),
                          std::forward<Tuple>(t),
                          std::make_index_sequence<tSize>());
}

template <typename T>
void handle(T v);

template <>
void handle(int v) {
  LOG(INFO) << "INT=" << v;
}

template <>
void handle(bool v) {
  LOG(INFO) << "BOOL=" << v;
}

template <>
void handle(char const* v) {
  LOG(INFO) << "STR=" << v;
}

template <>
void handle(double v) {
  LOG(INFO) << "FLOAT=" << v;
}

#define DI(I)                \
  if constexpr (size > I) {  \
    handle(std::get<I>(tp)); \
  }

template <typename Tuple>
decltype(auto) loop(Tuple&& tp) {
  std::size_t constexpr size = std::tuple_size<typename std::remove_reference_t<Tuple>>::value;
  DI(0)
  DI(1)
  DI(2)
  DI(3)
  DI(4)
  DI(5)
}

#undef DI

TEST(TupleTest, TestTuple) {
  auto sum = [](int a, int b, int c, int d) {
    LOG(INFO) << "a=" << a << ", b=" << b;
    return a + b + c + d;
  };

  int result = apply_from_tuple(sum, std::make_tuple(10, 20, 30, 40));
  EXPECT_EQ(result, 100);

  // test loop on each item
  auto t = std::make_tuple(10, true, "C++", 1.0);
  loop(t);
}

} // namespace test
} // namespace type
} // namespace nebula