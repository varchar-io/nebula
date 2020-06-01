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

#include <chrono>
#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <msgpack.hpp>
#include <roaring.hh>
#include <yaml-cpp/yaml.h>
#include <yorel/yomm2/cute.hpp>

#include "common/BloomFilter.h"
#include "common/Errors.h"
#include "common/Evidence.h"
#include "common/TaskScheduler.h"

/**
 * Test namespace for testing external dependency APIs
 */
namespace nebula {
namespace common {
namespace test {
// copied from the example of croaring page in github
TEST(RoraringTest, TestRoaringBitmap) {
  Roaring r1;
  for (uint32_t i = 100; i < 1000; i++) {
    r1.add(i);
  }

  // check whether a value is contained
  EXPECT_TRUE(r1.contains(500));
  LOG(INFO) << "r1  cardinality=" << r1.cardinality();

  // if your bitmaps have long runs, you can compress them by calling
  // run_optimize
  uint32_t size = r1.getSizeInBytes();
  r1.runOptimize();

  uint32_t compact_size = r1.getSizeInBytes();
  LOG(INFO) << fmt::format("size optimization: before={0}, after={1}.", size, compact_size);

  // create a new bitmap with varargs
  Roaring r2 = Roaring::bitmapOf(5, 1, 2, 3, 5, 6);
  LOG(INFO) << "r2: " << r2.toString();

  // we can also create a bitmap from a pointer to 32-bit integers
  const uint32_t values[] = { 2, 3, 4 };
  Roaring r3(3, values);

  // we can also go in reverse and go from arrays to bitmaps
  uint64_t card1 = r1.cardinality();
  uint32_t* arr1 = new uint32_t[card1];
  r1.toUint32Array(arr1);
  Roaring r1f(card1, arr1);
  delete[] arr1;

  // bitmaps shall be equal
  EXPECT_TRUE(r1 == r1f);

  // we can copy and compare bitmaps
  Roaring z(r3);
  EXPECT_TRUE(r3 == z);

  // we can compute union two-by-two
  Roaring r1_2_3 = r1 | r2;
  r1_2_3 |= r3;

  // we can compute a big union
  const Roaring* allmybitmaps[] = { &r1, &r2, &r3 };
  Roaring bigunion = Roaring::fastunion(3, allmybitmaps);
  EXPECT_TRUE(r1_2_3 == bigunion);

  // we can compute intersection two-by-two
  Roaring i1_2 = r1 & r2;

  // we can write a bitmap to a pointer and recover it later
  uint32_t expectedsize = r1.getSizeInBytes();
  char* serializedbytes = new char[expectedsize];
  r1.write(serializedbytes);
  Roaring t = Roaring::read(serializedbytes);
  EXPECT_TRUE(r1 == t);
  delete[] serializedbytes;

  // we can iterate over all values using custom functions
  uint32_t counter = 0;
  r1.iterate(
    [](uint32_t value, void* param) {
      *(uint32_t*)param += value;
      return true;
    },
    &counter);

  // we can move iterators to skip values
  const uint32_t manyvalues[] = { 2, 3, 4, 7, 8 };
  Roaring rogue(5, manyvalues);
  Roaring::const_iterator j = rogue.begin();
  j.equalorlarger(4);
  EXPECT_EQ(*j, 4);
}

TEST(CuckooTest, TestCuckooFilter) {
  size_t items = 100000;

  // Create a cuckoo filter where each item is of type size_t and
  // use 12 bits for each item:
  //    CuckooFilter<size_t, 12> filter(total_items);
  // To enable semi-sorting, define the storage of cuckoo filter to be
  // PackedTable, accepting keys of size_t type and making 13 bits
  // for each key:
  //   CuckooFilter<size_t, 13, cuckoofilter::PackedTable> filter(total_items);
  BloomFilter<int64_t> filter(items);
  BloomFilter<int64_t> filter1(items);
  BloomFilter<int64_t> filter2(items);
  auto rand = nebula::common::Evidence::rand<int64_t>(0, std::numeric_limits<int64_t>::max());

  // Insert items to this cuckoo filter
  int64_t base = 301882118680623300;
  for (size_t i = 0; i < items; ++i) {
    if (!filter.add(i)) {
      LOG(INFO) << "failed to add item at " << i;
      break;
    }

    if (!filter1.add(base)) {
      LOG(INFO) << "failed to add item at " << i;
      break;
    }

    if (!filter2.add(base + rand())) {
      LOG(INFO) << "failed to add item at " << i;
      break;
    }
  }

  // Check if previously inserted items are in the filter, expected
  // true for all items
  // for (size_t i = 0; i < num_inserted; i++) {
  //   EXPECT_TRUE(filter.probably(i));
  // }

  // Check non-existing items, a few false positives expected
  size_t total_queries = 0;
  size_t false_queries = 0;
  for (size_t i = items; i < 2 * items; i++) {
    if (filter.probably(i)) {
      false_queries++;
    }

    total_queries++;
  }

  // Output the measured false positive rate
  LOG(INFO) << "false positive rate is "
            << 100.0 * false_queries / total_queries
            << " filter size: " << filter.bytes();
}

// Testing yomm2 open multi-methods
struct A {
  virtual ~A() {}
};
struct B : A {};
struct C : A {};

register_class(A);
register_class(B, A);
register_class(C, A);

declare_method(std::string, foobar, (yorel::yomm2::virtual_<A&>));

define_method(std::string, foobar, (A&)) {
  return "foobar(A)";
}

define_method(std::string, foobar, (B&)) {
  return "foobar(B)";
}

// define_method(std::string, foobar, (C&)) {
//   return "foobar(C)";
// }

TEST(CommonTest, TestOmm) {
  yorel::yomm2::update_methods();

  // NOTE: multi-inheritence may not work as tested.
  B b;
  C c;
  LOG(INFO) << foobar(b);
  LOG(INFO) << foobar(c);
}

#define TYPE_PRINT(T)       \
  case YAML::NodeType::T: { \
    LOG(INFO) << #T;        \
    break;                  \
  }

void printType(const YAML::Node& n) {
  switch (n.Type()) {
    TYPE_PRINT(Undefined)
    TYPE_PRINT(Null)
    TYPE_PRINT(Scalar)
    TYPE_PRINT(Sequence)
    TYPE_PRINT(Map)
  default:
    throw NException("unknown type");
  }
}
#undef TYPE_PRINT

TEST(CommonTest, TestYamlParser) {
  YAML::Node config = YAML::LoadFile("configs/test.yml");

  printType(config);
  LOG(INFO) << "Version=" << config["version"].as<std::string>();

  const auto& nodes = config["nodes"];
  LOG(INFO) << "Root=" << nodes.size();
  printType(nodes);
}

TEST(CommonTest, TestTimer) {
  nebula::common::TaskScheduler scheduler;
  // scheduler.setInterval(1000, [] { LOG(INFO) << "Did Interval"; });
  scheduler.setTimeout(2000, [] { LOG(INFO) << "Did Timeout"; });

  // run scheduler
  scheduler.run();

  // after done
  LOG(INFO) << "scheduler exits";
}

TEST(CommonTest, TestMsgpack) {
  {
    using TT = std::tuple<int, bool, std::string>;
    TT src(1, true, "example");

    std::stringstream buffer;
    msgpack::pack(buffer, src);

    // send the buffer ...
    buffer.seekg(0);

    // unpack
    std::string str = buffer.str();
    msgpack::object_handle oh = msgpack::unpack(str.data(), str.size());

    msgpack::object deserialized = oh.get();
    LOG(INFO) << deserialized;

    TT dst;
    deserialized.convert(dst);

    // or create the new instance
    TT dst2 = deserialized.as<TT>();
    EXPECT_EQ(dst, dst2);
  }

  // pack a hash map
  {
    std::unordered_map<std::string, std::string> map = { { "abc", "xyz" }, { "123", "456" } };
    std::stringstream buffer2;
    msgpack::pack(buffer2, map);
    buffer2.seekg(0);
    std::string result = buffer2.str();
    msgpack::object_handle oh2 = msgpack::unpack(result.data(), result.size());
    std::unordered_map<std::string, std::string> map2;
    oh2.get().convert(map2);
    LOG(INFO) << "abc: " << map2["abc"];
  }

  // single double tuple
  {
    using TT = std::tuple<double>;
    TT src(99);

    std::stringstream buffer;
    msgpack::pack(buffer, src);

    // send the buffer ...
    buffer.seekg(0);

    // unpack
    std::string str = buffer.str();
    LOG(INFO) << "ser=" << str << ", size=" << str.size();
    msgpack::object_handle oh = msgpack::unpack(str.data(), str.size());

    msgpack::object deserialized = oh.get();
    LOG(INFO) << "des=" << deserialized;
  }
}

} // namespace test
} // namespace common
} // namespace nebula