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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <regex>
#include <valarray>
#include <xxh3.h>

#include "common/Chars.h"
#include "common/Errors.h"
#include "common/Evidence.h"
#include "common/Fold.h"
#include "common/Hash.h"
#include "common/Int128.h"
#include "common/Likely.h"
#include "common/Memory.h"

namespace nebula {
namespace common {
namespace test {

using nebula::common::Int128_U;

TEST(CommonTest, TestEnsures) {
  // test generic ensure
  N_ENSURE(3 > 2, "true");
  try {
    N_ENSURE(2 > 3, "false");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.what();
  }

  // test ensure equals
  N_ENSURE_EQ((3 + 3), 6, "3+3 should be 6");
  try {
    N_ENSURE_EQ((3 + 3), 7, "3+3 should not be 7");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.what();
  }

  // test ensure greater or equals
  N_ENSURE_GE((3 + 3), 6, "3+3 should be 6");
  N_ENSURE_GT((3 + 3), 5, "3+3 should greater than 5");
  try {
    N_ENSURE_GE((3 + 3), 7, "3+3 should not be 7");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.what();
  }

  // test ensure less than
  N_ENSURE_LE((3 + 3), 6, "3+3 should be 6");
  N_ENSURE_LT((3 + 3), 7, "3+3 should less than 7");
  try {
    N_ENSURE_LT((3 + 3), 6, "3+3 should not be less than 6");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.what();
  }

  // test
  N_ENSURE_NE(3 + 3, 5, "3+3 not equal 5");
  try {
    N_ENSURE_NE((3 + 3), 6, "3+3 should equal 6");
  } catch (const NebulaException& ex) {
    LOG(INFO) << ex.what();
  }
}

TEST(CommonTest, TestValArray) {
  std::valarray<double> a(1, 8);
  std::valarray<double> b{ 1, 2, 3, 4, 5, 6, 7, 8 };
  std::valarray<double> c = -b;

  // literals must also be of type T (double in this case)
  std::valarray<double> d = std::sqrt((b * b - 4.0 * a * c));
  std::valarray<double> x1 = (-b - d) / (2.0 * a);
  std::valarray<double> x2 = (-b + d) / (2.0 * a);
  LOG(INFO) << "quadratic equation    root 1,  root 2";
  for (size_t i = 0; i < a.size(); ++i) {
    LOG(INFO) << a[i] << "x\u00B2 + " << b[i] << "x + " << c[i] << " = 0   " << x1[i] << ", " << x2[i];
  }
}

struct TestPadding {
  bool is_cached_{};
  double rank_{};
  int id_{};
};

struct TestVirualPadding {
  bool is_cached_{};
  char padding_bool_[7];
  double rank_{};
  int id_{};
  char padding_int_[4];
};

struct OptPadding {
  bool is_cached_{};
  int id_{};
  double rank_{};
};

struct OptPadding2 {
  double rank_{};
  int id_{};
  bool is_cached_{};
};

TEST(CommonTest, TestNewDeleteOps) {
  auto* p = new char{ 'a' };
  *p = 10;
  delete p;

  // alignment information
  LOG(INFO) << "align of int: " << alignof(int) << ", align of double: " << alignof(double);

  // size of object due to padding
  auto size1 = sizeof(TestPadding);
  LOG(INFO) << "size of TestPadding: " << size1;

  auto size2 = sizeof(TestVirualPadding);
  LOG(INFO) << "size of TestVirtualPadding: " << size2;

  // A class padding/alignment requirement determined by the largest member alignment (double) here
  EXPECT_EQ(size1, size2);

  // keep member layout in the order of its size
  auto size3 = sizeof(OptPadding);
  LOG(INFO) << "size of OptPadding: " << size3;
  EXPECT_EQ(size3, 16);

  auto size4 = sizeof(OptPadding2);
  LOG(INFO) << "size of OptPadding2: " << size4;
  EXPECT_EQ(size4, 16);
}

TEST(CommonTest, TestSliceAndExtendableSlice) {
  nebula::common::ExtendableSlice slice(1024);
  EXPECT_EQ(slice.capacity(), 1024);

  size_t cursor = 0;
  // write all differnt types of data and check their size
  // LOG(INFO)<< "is bool scalar: " << std::is_scalar<bool>::value;
  cursor += slice.write(cursor, true);
  EXPECT_EQ(cursor, 1);
  cursor += slice.write(cursor, 'a');
  EXPECT_EQ(cursor, 2);
  short s = 1;
  cursor += slice.write(cursor, s);
  EXPECT_EQ(cursor, 4);
  cursor += slice.write(cursor, 3);
  EXPECT_EQ(cursor, 8);
  cursor += slice.write(cursor, 8L);
  EXPECT_EQ(cursor, 16);

  cursor += slice.write(cursor, 3.2f);
  EXPECT_EQ(cursor, 20);
  cursor += slice.write(cursor, 6.4);
  EXPECT_EQ(cursor, 28);
  cursor += slice.write(cursor, 8.9L);
  EXPECT_EQ(cursor, 44);
  auto str = "abcxyz";
  cursor += slice.write(cursor, str, std::strlen(str));
  EXPECT_EQ(cursor, 50);

  int128_t i128 = 16;
  cursor += slice.write(cursor, i128);
  EXPECT_EQ(cursor, 66);

  // read all data written above
  EXPECT_EQ(slice.read<bool>(0), true);
  EXPECT_EQ(slice.read<char>(1), 'a');
  EXPECT_EQ(slice.read<short>(2), 1);
  EXPECT_EQ(slice.read<int>(4), 3);
  EXPECT_EQ(slice.read<long>(8), 8L);
  EXPECT_EQ(slice.read<float>(16), 3.2f);
  EXPECT_EQ(slice.read<double>(20), 6.4);
  EXPECT_EQ(slice.read<long double>(28), 8.9L);
  EXPECT_EQ(slice.read(44, 6), "abcxyz");
  EXPECT_EQ(slice.read<int128_t>(50), i128);

  // write to position overflow sinle slice - paged slice will auto grow
  slice.write(1050, 1.0);
  EXPECT_EQ(slice.read<double>(1050), 1.0);
  EXPECT_EQ(slice.capacity(), 2048);
}

TEST(CommonTest, TestSliceWrite) {
  nebula::common::ExtendableSlice slice(1024);

  auto value = 1699213050;
  slice.write(0, value);
  EXPECT_EQ(slice.read<int>(0), value);
}

TEST(CommonTest, TestTimeParsing) {
  LOG(INFO) << "2019-04-01 = " << Evidence::time("2019-04-01", "%Y-%m-%d");

  {
    auto time1 = Evidence::time("2019-04-01 23:23:45", "%Y-%m-%d %H:%M:%S");
    auto time2 = Evidence::time("2019-04-01 00:00:00", "%Y-%m-%d %H:%M:%S");
    LOG(INFO) << "time1: " << time1 << ", time2:" << time2;
    EXPECT_EQ(Evidence::date(time1), time2);
  }

  {
    auto time1 = Evidence::time("2019-03-31 00:45:22", "%Y-%m-%d %H:%M:%S");
    auto time2 = Evidence::time("2019-03-31 00:00:00", "%Y-%m-%d %H:%M:%S");
    LOG(INFO) << "time1: " << time1 << ", time2:" << time2;
    EXPECT_EQ(Evidence::date(time1), time2);
  }
}

TEST(CommonTest, TestTimeFormatting) {
  auto time = std::time(nullptr);
  LOG(INFO) << Evidence::fmt_extra(time);
  LOG(INFO) << Evidence::fmt_mdy_dash(time);
  LOG(INFO) << Evidence::fmt_mdy_slash(time);
  LOG(INFO) << Evidence::fmt_normal(time);
  LOG(INFO) << Evidence::fmt_ymd_dash(time);
  LOG(INFO) << Evidence::fmt_ymd_slash(time);

  // shortcuts are resulting in the same
  EXPECT_EQ(Evidence::fmt_ymd_dash(time), Evidence::format(time, "%F"));
  EXPECT_EQ(Evidence::fmt_mdy2_slash(time), Evidence::format(time, "%D"));
}

TEST(CommonTest, TestRand) {
  LOG(INFO) << "unix time now: " << Evidence::unix_timestamp();

  // a range of [0,100]
  auto rand1 = Evidence::rand(0, 100);
  for (auto i = 0; i < 1000; ++i) {
    size_t val = rand1();
    EXPECT_GE(val, 0);
    EXPECT_LE(val, 100);
    if (UNLIKELY(val < 2)) {
      LOG(INFO) << "val: " << val;
    }
  }

  LOG(INFO) << "finish";
  auto rand2 = Evidence::rand();
  for (auto i = 0; i < 1000; ++i) {
    auto val = rand2();
    EXPECT_TRUE(val >= 0 && val < 1);
    if (UNLIKELY(val > 0.98)) {
      LOG(INFO) << "val: " << val;
    }
  }
}

TEST(CommonTest, TestStringView) {
  auto str = "abcdefg";
  auto str2 = "ab";
  std::string_view v1(str, 2);
  EXPECT_EQ(v1, str2);
}

TEST(CommonTest, TestStdRegex) {
  std::string input = "(((F:_time_>C:1546300800)&&(F:_time_<C:1556668800))&&(F:id==C:1000000))";
  std::regex phaseSearch("TIME", std::regex_constants::ECMAScript | std::regex_constants::icase);
  EXPECT_TRUE(std::regex_search(input, phaseSearch));

  // replace any match with bracket
  std::regex underscore_regex("(_\\w+_)");
  std::string another = std::regex_replace(input, underscore_regex, "[$&]");
  EXPECT_EQ(another, "(((F:[_time_]>C:1546300800)&&(F:[_time_]<C:1556668800))&&(F:id==C:1000000))");

  // list all matches
  std::regex col_regex("&&\\(F:(\\w+)==C:(\\w+)\\)\\)");
  std::smatch matches;
  if (std::regex_search(input, matches, col_regex)) {
    for (size_t i = 0; i < matches.size(); ++i) {
      LOG(INFO) << matches[i].str();
    }
  }

  // use iterater to get all matches
  auto m_begin = std::sregex_iterator(input.begin(), input.end(), col_regex);
  auto m_end = std::sregex_iterator();
  for (auto i = m_begin; i != m_end; ++i) {
    std::smatch match = *i;
    std::string match_str = match.str();
    LOG(INFO) << match_str;
  }
}

TEST(CommonTest, TestXxhSpeed) {
  // comapre to std::hash, let's understand the perf gap
  const std::string str = "what is the hash value of nebula";
  const size_t iterations = 1000000;
  std::hash<std::string> hasher;
  nebula::common::Evidence::Duration duration;
  using nebula::common::Hasher;
  for (size_t i = 0; i < iterations; ++i) {
    hasher(str);
  }
  LOG(INFO) << "std::hash<string> hash " << iterations << " times using ms: " << duration.elapsedMs() << " hash:" << hasher(str);
  duration.reset();
  const void* p = str.data();
  const auto len = str.size();
  for (size_t i = 0; i < iterations; ++i) {
    XXH3_64bits(p, len);
  }
  LOG(INFO) << "xxh3<string> hash " << iterations << " times using ms: " << duration.elapsedMs() << " hash: " << XXH3_64bits(p, len);

  // using our own wrapper
  duration.reset();
  for (size_t i = 0; i < iterations; ++i) {
    Hasher::hashString(str);
  }
  LOG(INFO) << "nebula-hahser<string> hash " << iterations << " times using ms: " << duration.elapsedMs() << " hash: " << Hasher::hashString(str);

  LOG(INFO) << " XXH VECTOR: " << XXH_VECTOR;
}
inline bool f1ni(int i) {
  return i % 2 == 0;
}

TEST(CommonTest, TestOptionalPerf) {
  // use -1 indicating null
  // auto f1n = [](int i) -> bool {
  //   return i % 2 == 0;
  // };
  // TEST REPORT:
  // Mac (2.6 GHz Intel Core i7)
  // Compiler: Apple LLVM version 10.0.1 (clang-1001.0.46.4)s
  // In Debug mode, std::optional is always 10x slower than direct method
  // in Release mode, std::optional is 2 times slower
  // I0809 13:10:01.154446 209200576 TestCommon.cpp:340] optional approach: sum=2500000050000000, time=32
  // I0809 13:10:01.170831 209200576 TestCommon.cpp:349] special value approach: sum=2500000050000000, time=15

  // Linux (Intel(R) Xeon(R) Platinum 8124M CPU @ 3.00GHz)
  // Compiler: linux: g++ (Ubuntu 8.1.0-5ubuntu1~14.04) 8.1.0
  // However in release mode on linux (-o3), std::optional version is always faster (59ms vs 81ms)
  // no matter how we write the for loop, using f1n, f1ni or direct condition
  // Given our target platform is linux release mode, using std::optional in our row interface seems reasonable

  // I would like to try latest clang on linux to see if there is differences
  // follow this to get clang 8.0
  // https://solarianprogrammer.com/2017/12/13/linux-wsl-install-clang-libcpp-compile-cpp-17-programs/

  auto f1 = [](int i) -> int {
    return i + 1;
  };

  auto f2 = [](int i) -> std::optional<int> {
    if (i % 2 == 0) {
      return std::nullopt;
    }
    return i + 1;
  };

  // run 1M cycles
  constexpr auto cycles = 100'000'000;
  nebula::common::Evidence::Duration duration;
  long sum2 = 0;
  for (int i = 0; i < cycles; ++i) {
    auto x = f2(i);
    if (x) {
      sum2 += x.value();
    }
  }
  LOG(INFO) << fmt::format("optional approach: sum={0}, time={1}", sum2, duration.elapsedMs());

  duration.reset();
  long sum1 = 0;
  for (int i = 0; i < cycles; ++i) {
    if (i % 2 != 0) {
      sum1 += f1(i);
    }
  }
  LOG(INFO) << fmt::format("special value approach: sum={0}, time={1}", sum1, duration.elapsedMs());
  EXPECT_EQ(sum1, sum2);
}

TEST(CommonTest, TestNamedFormat) {
  {
    auto str = fmt::format("Hello, {name}! Goodbye, {name}.", fmt::arg("name", "World"));
    EXPECT_EQ(str, "Hello, World! Goodbye, World.");
  }
  {
    auto str = fmt::format("Hello, {name}!", fmt::arg("notfit", "World"), fmt::arg("name", "Nebula"));
    EXPECT_EQ(str, "Hello, Nebula!");
  }
}

TEST(CommonTest, TestMultiFold) {
  // test different data size for the algo
  folly::CPUThreadPoolExecutor pool{ 8 };
  for (auto size = 1; size < 1000; size *= 2) {
    LOG(INFO) << "Test multi-fold with size = " << size;

    // fill sequences in the values vector
    std::vector<int> values(size);
    std::generate(values.begin(), values.end(), [n = 0]() mutable { return n++; });

    nebula::common::fold<int>(pool, values, [](int& from, int& to) {
      to += from;
    });

    auto sum = 0;
    for (auto i = 0; i < size; ++i) {
      sum += i;
    }

    EXPECT_EQ(values.at(0), sum);
  }
}

TEST(CommonTest, TestInt128) {
  int128_t x = 128;
  LOG(INFO) << "is int128 scalar: " << std::is_scalar<int128_t>::value << ", size=" << sizeof(int128_t);
  int128_t y = std::numeric_limits<__int128_t>::max();
  EXPECT_TRUE(y > x);
  LOG(INFO) << "x=" << x << ", y=" << y;

  y += 1;
  EXPECT_TRUE(x > y);

  int128_t z = Int128_U::UINT128_LOW_MASK;
  int128_t z1 = z + 1;

  // test out int128 features
  EXPECT_EQ(Int128_U::low64<int64_t>(z1), 0);
  EXPECT_EQ(Int128_U::high64<int64_t>(z1), 1);

  EXPECT_TRUE(z1 > z);
  auto z2 = z1 >> 64;
  EXPECT_TRUE(z2 == 1);
  LOG(INFO) << "z=" << z << ", z1=" << z1 << ", z2=" << z2;

  // lower part is 12804 and high part is still 1
  auto delta = 12804;
  z1 += delta;

  // try different types
  EXPECT_EQ(Int128_U::low64<int64_t>(z1), delta);
  EXPECT_EQ(Int128_U::high64<int64_t>(z1), 1);

  EXPECT_EQ(Int128_U::low64<uint32_t>(z1), delta);
  EXPECT_EQ(Int128_U::high64<uint32_t>(z1), 1);

  EXPECT_EQ(Int128_U::low64<int32_t>(z1), delta);
  EXPECT_EQ(Int128_U::high64<int32_t>(z1), 1);

  EXPECT_EQ(Int128_U::low64<uint16_t>(z1), delta);
  EXPECT_EQ(Int128_U::high64<uint16_t>(z1), 1);

  EXPECT_EQ(Int128_U::low64<int16_t>(z1), delta);
  EXPECT_EQ(Int128_U::high64<int16_t>(z1), 1);

  // now I want to mask high part to turn it into a negative value
  int128_t zn = z1 | Int128_U::UINT128_HIGH_MASK;
  EXPECT_TRUE(zn < 0);

  // by default converting to uint64
  EXPECT_EQ(Int128_U::low64<int64_t>(zn), delta);
  EXPECT_EQ(Int128_U::high64<int64_t>(zn), UINT64_MAX);

  // for signed int64, it is a negative value
  EXPECT_EQ(Int128_U::low64<int64_t>(zn), delta);
  EXPECT_EQ(Int128_U::high64<int64_t>(zn), -1);
  LOG(INFO) << "zn=" << zn;

  // test value increment on different section
  {
    int128_t parts = 0;
    int64_t sum = 0;
    int64_t count = 1000;
    for (int64_t i = 0; i < count; ++i) {
      Int128_U::high64_add(parts, i);
      sum += i;
      Int128_U::low64_add(parts, 1);
    }

    EXPECT_EQ(Int128_U::high64<int64_t>(parts), sum);
    EXPECT_EQ(Int128_U::low64<int64_t>(parts), count);
  }

  {
    // pointer has type, which impacts the address incremental
    int128_t parts = 0;
    double sum = 0;
    int64_t count = 1000;
    for (int64_t i = 0; i < count; ++i) {
      Int128_U::high64_add(parts, i * 1.1);
      sum += i * 1.1;
      Int128_U::low64_add(parts, 1);
    }

    EXPECT_EQ(Int128_U::high64<double>(parts), sum);
    EXPECT_EQ(Int128_U::low64<int64_t>(parts), count);
  }

  // test raw operations on bytes
  {
    int128_t i128 = { 0 };
    // WRONG: auto addr = &i128;
    // addr + 1 will translate into add 16 bytes (size of *ptr)
    auto addr = reinterpret_cast<int8_t*>(&i128);
    LOG(INFO) << "i128 address=" << addr << ": " << i128;
    for (int i = 0; i < 16; i++) {
      LOG(INFO) << addr + i << ": byte=" << (int)*(addr + i);
    }

    int64_t* low = reinterpret_cast<int64_t*>(addr);
    // because of low is 8 bytes
    int64_t* high = low + 1;
    int64_t sum = 0;
    int64_t count = 0;
    LOG(INFO) << "low=" << *low
              << ", high=" << *high
              << ", sum=" << sum
              << ", count=" << count;

    for (int i = 0; i < 1111; ++i) {
      *high += i;
      *low += 1;

      sum += i;
      count += 1;
    }

    LOG(INFO) << "low=" << *low
              << ", high=" << *high
              << ", sum=" << sum
              << ", count=" << count;

    union U {
      int128_t a;
      int8_t b[16];
    };

    U u = { 0 };
    LOG(INFO) << "value=" << *reinterpret_cast<int64_t*>(u.b + 8);
    for (int i = 0; i < 16; i++) {
      LOG(INFO) << "byte=" << (int)u.b[i];
    }
  }

  {
    // test raw operations on bytes for double
    int128_t i128 = { 0 };
    auto addr = reinterpret_cast<int8_t*>(&i128);
    int64_t* low = reinterpret_cast<int64_t*>(addr);
    // addr is byte pointer, so +8 will move 8 bytes
    double* high = reinterpret_cast<double*>(addr + 8);

    double sum = 0;
    int64_t count = 0;
    LOG(INFO) << "low=" << *low
              << ", high=" << *high
              << ", sum=" << sum
              << ", count=" << count;

    for (int i = 0; i < 1111; ++i) {
      *high += i * 1.3;
      *low += 1;

      sum += i * 1.3;
      count += 1;
    }

    LOG(INFO) << "int128=" << i128
              << ", low=" << *low
              << ", high=" << *high
              << ", sum=" << sum
              << ", count=" << count;
  }
}

TEST(CommonTest, TestCharsUtils) {
#define TEST_SPLIT(EXPECTED, COUNT, D)                                                          \
  auto result = nebula::common::Chars::split(str, strlen(str), D);                              \
  EXPECT_EQ(result.size(), COUNT);                                                              \
  std::vector<std::string> list;                                                                \
  list.reserve(COUNT);                                                                          \
  std::transform(result.begin(), result.end(), std::back_insert_iterator(list), [](auto& str) { \
    return str;                                                                                 \
  });                                                                                           \
  std::sort(list.begin(), list.end());                                                          \
  EXPECT_EQ(list, EXPECTED);

  {
    auto str = "xyz,abc,";
    std::vector<std::string> expected{ "abc", "xyz" };
    TEST_SPLIT(expected, 2, ',')
  }
  {
    auto str = ",ayz,abc,";
    std::vector<std::string> expected{ "abc", "ayz" };
    TEST_SPLIT(expected, 2, ',')
  }
  {
    auto str = ",";
    std::vector<std::string> expected{};
    TEST_SPLIT(expected, 0, ',')
  }
  {
    auto str = ",,,x,a,z,,";
    std::vector<std::string> expected{ "a", "x", "z" };
    TEST_SPLIT(expected, 3, ',')
  }
  {
    auto str = ",a,a,a,b,x,b,x,x,,";
    std::vector<std::string> expected{ "a", "b", "x" };
    TEST_SPLIT(expected, 3, ',')
  }
  {
    auto str = "abc:#x:m&:";
    std::vector<std::string> expected{ "#x", "abc", "m&" };
    TEST_SPLIT(expected, 3, ':')
  }
  {
    auto str = "abc,xyz";
    std::vector<std::string> expected{ "abc", "xyz" };
    TEST_SPLIT(expected, 2, ',')
  }
  {
    auto str = "abc";
    std::vector<std::string> expected{ "abc" };
    TEST_SPLIT(expected, 1, ',')
  }

#undef TEST_SPLIT
}

TEST(CommonTest, TestRange) {
  nebula::common::PRange r1;
  EXPECT_EQ(r1.offset, 0);
  EXPECT_EQ(r1.size, 0);

  nebula::common::PRange r2(2, 100);
  EXPECT_EQ(r2.offset, 2);
  EXPECT_EQ(r2.size, 100);

  nebula::common::ExtendableSlice slice(1024);
  nebula::common::PRange r3(111, 222);
  EXPECT_EQ(r3.write(slice, 23), 8);

  auto r4 = nebula::common::PRange::make(slice, 23);
  EXPECT_EQ(r4.offset, 111);
  EXPECT_EQ(r4.size, 222);

  auto max = std::numeric_limits<uint32_t>::max();
  nebula::common::PRange r5(max, max);
  EXPECT_EQ(r5.write(slice, 55), 8);

  nebula::common::PRange r6;
  r6.read(slice, 55);
  EXPECT_EQ(r6.offset, max);
  EXPECT_EQ(r6.size, max);
}

TEST(CommonTest, TestWriteAlign) {
  nebula::common::ExtendableSlice slice(1024);
  int8_t v = 126;
  constexpr auto alignment = 8;
  constexpr auto offset = 198;
  size_t len = slice.writeAlign(offset, v, alignment);
  EXPECT_EQ(len, alignment);
  auto v2 = slice.read<int8_t>(offset);
  EXPECT_EQ(v, v2);
}

struct B {
  B() {
    LOG(INFO) << "CONSTRUCT B";
  }
  virtual ~B() {
    LOG(INFO) << "DESTRUCT B";
  }
};
struct S : public B {
  S() {
    LOG(INFO) << "CONSTRUCT S";
  }
  ~S() = default;
};

TEST(CommonTest, TestConstructDestructOrder) {
  auto s = std::make_unique<S>();
  LOG(INFO) << (s != nullptr);
  s = nullptr;
  LOG(INFO) << "destructor already called";
}
} // namespace test
} // namespace common
} // namespace nebula