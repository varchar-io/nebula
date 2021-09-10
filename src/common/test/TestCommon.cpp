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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/writer.h>
#include <regex>
#include <valarray>
#include <xxh3.h>

#include "common/Bits.h"
#include "common/Chars.h"
#include "common/Conv.h"
#include "common/Errors.h"
#include "common/Evidence.h"
#include "common/Finally.h"
#include "common/Fold.h"
#include "common/Format.h"
#include "common/Hash.h"
#include "common/IStream.h"
#include "common/Int128.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "common/Params.h"
#include "common/Spark.h"
#include "common/StackTree.h"

namespace nebula {
namespace common {
namespace test {

using nebula::common::Int128_U;

TEST(CommonTest, TestGTestFunc) {
  struct TestObj {
    explicit TestObj(int k) : kind{ k } {}
    int kind;
  };

  TestObj o{ 0 };
  LOG(INFO) << ::testing::PrintToString(o);
}

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
  EXPECT_EQ(slice.size(), 1024);

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
  EXPECT_TRUE(slice.read<int128_t>(50) == i128);

  // write to position overflow sinle slice - paged slice will auto grow
  slice.write(1050, 1.0);
  EXPECT_EQ(slice.read<double>(1050), 1.0);
  EXPECT_EQ(slice.size(), 2048);
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

  // time pattern with AM/PM
  {
    auto time1 = Evidence::time("07/20/2013 14:33:47 PM", "%m/%d/%Y %H:%M:%S");
    LOG(INFO) << "time1: " << time1;
    auto time2 = Evidence::time("ENDANGERED", "%m/%d/%Y %H:%M:%S");
    LOG(INFO) << "time2: " << time2;
  }
}

TEST(CommonTest, Test0AndOverflow) {
  int64_t min = 0, max = 0;
  size_t size = 100;
  const size_t delta = max - min;
  if (delta < size) {
    max = min + size;
  }

  LOG(INFO) << "min=" << min << ", max=" << max << ", delta=" << delta;
  EXPECT_TRUE(min < max);
}

TEST(CommonTest, TestSerialNumberTime) {
  auto serial = 43386;
  auto date = "10/13/2018";
  auto time1 = Evidence::time(date, "%m/%d/%Y");
  auto time2 = Evidence::serial_2_unix(serial);
  EXPECT_EQ(time1, time2);
}

TEST(CommonTest, TestNormalize) {
  // need a function to be consistent between JS and C++
  // to normalize a string literal
  // text.replace(/[\W_]+/g, '_')
  std::regex to_underscore("[\\W_]+");
  auto text1 = "Abc Xyz";
  auto r_text1 = std::regex_replace(text1, to_underscore, "_");
  EXPECT_EQ(r_text1, "Abc_Xyz");

  // normalize is similar to above regex
  nebula::common::unordered_map<std::string, std::string> tests = {
    { "a Deer!", "a_deer_" },
    { "#$%X-RAY?!", "_x_ray_" },
    { "how! aRe you??", "how_are_you_" },
    { "#$%@#", "_" },
    { "___123_#$%X___456_xyz?!", "_123_x_456_xyz_" },
    { "reason ", "reason_" }
  };

  for (auto itr = tests.begin(); itr != tests.end(); ++itr) {
    EXPECT_EQ(nebula::common::normalize(itr->first), itr->second);
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
  LOG(INFO) << Evidence::fmt_hour(time);
  LOG(INFO) << Evidence::fmt_minute(time);
  LOG(INFO) << Evidence::fmt_second(time);
  LOG(INFO) << time;

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
    if (N_UNLIKELY(val < 2)) {
      LOG(INFO) << "val: " << val;
    }
  }

  LOG(INFO) << "finish";
  auto rand2 = Evidence::rand();
  for (auto i = 0; i < 1000; ++i) {
    auto val = rand2();
    EXPECT_TRUE(val >= 0 && val < 1);
    if (N_UNLIKELY(val > 0.98)) {
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

TEST(CommonTest, TestOptionalBool) {
  std::optional<int32_t> x = 0;
  std::optional<int8_t> y = x;
  EXPECT_TRUE(x);
  EXPECT_TRUE(y);

  auto f0 = []() -> std::optional<int8_t> {
    return 0;
  };
  EXPECT_FALSE(!f0());

  auto f1 = []() -> std::optional<bool> {
    return std::nullopt;
  };
  EXPECT_EQ(f1(), std::nullopt);
  EXPECT_FALSE(f1());

  auto f2 = []() -> std::optional<bool> {
    return false;
  };
  EXPECT_EQ(f2(), false);

  auto f3 = []() -> std::optional<bool> {
    return true;
  };
  EXPECT_EQ(f3(), true);
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
  {
    auto str = fmt::format("{{{0}}}", "NEBULA");
    EXPECT_EQ(str, "{NEBULA}");
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

TEST(CommonTest, TestCaseConvertion) {
  std::string x = "AbC";
  auto another = nebula::common::Chars::lower_copy(x);
  EXPECT_EQ(x, "AbC");
  EXPECT_EQ(another, "abc");

  nebula::common::Chars::lower(x);
  EXPECT_EQ(x, "abc");

  auto copy = nebula::common::Chars::lower_copy("XYz");
  EXPECT_EQ(copy, "xyz");
}

TEST(CommonTest, TestDigest) {
  {
    auto url = "https://nebula.com/questions/27370480/how-to-simplify";
    auto digest = nebula::common::Chars::digest(url, std::strlen(url));
    EXPECT_EQ(digest.size(), 6);

    LOG(INFO) << "digest=" << digest;
  }
  {
    auto url = "http";
    auto digest = nebula::common::Chars::digest(url, std::strlen(url));
    EXPECT_EQ(digest.size(), 4);
    EXPECT_EQ(digest, url);
  }
  {
    auto url = "https://en.wikipedia.org/wiki/Bijection";
    auto digest = nebula::common::Chars::digest(url, std::strlen(url));
    EXPECT_EQ(digest.size(), 6);

    LOG(INFO) << "digest=" << digest;
  }
  {
    auto url = "/#%7B%22table%22%3A%22nebula.test%22%2C%22start%22%3A%222019-08-16%2022%3A23%3A14%22%2C%22end%22%3A%222019-08-26%2002%3A23%3A14%22%2C%22filter%22%3A%7B%22l%22%3A%22AND%22%2C%22r%22%3A%5B%7B%22c%22%3A%22event%22%2C%22o%22%3A%224%22%2C%22v%22%3A%5B%22NN%25%22%5D%7D%5D%2C%22g%22%3A%5B%5D%7D%2C%22keys%22%3A%5B%22tag%22%5D%2C%22window%22%3A%220%22%2C%22display%22%3A%220%22%2C%22metrics%22%3A%5B%5D%2C%22sort%22%3A%220%22%2C%22limit%22%3A%2210%22%7D";
    auto digest = nebula::common::Chars::digest(url, std::strlen(url));
    EXPECT_EQ(digest.size(), 6);
    LOG(INFO) << "digest=" << digest;
  }
}

TEST(CommonTest, TestPath) {
  {
    auto str = "a.b.c";
    EXPECT_EQ(nebula::common::Chars::path(str, strlen(str)), "/a/b/c");
  }
  {
    auto str = "a";
    EXPECT_EQ(nebula::common::Chars::path(str, strlen(str)), "/a");
  }
  {
    auto str = "";
    EXPECT_EQ(nebula::common::Chars::path(str, strlen(str)), "/");
  }
  {
    auto str = "a.b.";
    EXPECT_EQ(nebula::common::Chars::path(str, strlen(str)), "/a/b/");
  }
  {
    auto str = "a..b";
    EXPECT_EQ(nebula::common::Chars::path(str, strlen(str)), "/a//b");
  }
}

TEST(CommonTest, TestCharsSplitInOrder) {
  {
    auto str = "xyz,abc,";
    std::vector<std::string> expected{ "xyz", "abc" };
    auto result = nebula::common::Chars::split<true>(str, strlen(str), ',');
    EXPECT_EQ(result, expected);
  }
  {
    auto str = "abc,xyz,123,opq,456";
    std::vector<std::string> expected{ "abc", "xyz", "123", "opq", "456" };
    auto result = nebula::common::Chars::split<true>(str, strlen(str), ',');
    EXPECT_EQ(result, expected);
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

TEST(CommonTest, TestCharsLast) {
  auto str1 = "abc/";
  EXPECT_EQ(nebula::common::Chars::last(str1), "");
  auto str2 = "abc";
  EXPECT_EQ(nebula::common::Chars::last(str2), str2);
  auto str3 = "";
  EXPECT_EQ(nebula::common::Chars::last(str3), str3);
  auto str4 = "/abc";
  EXPECT_EQ(nebula::common::Chars::last(str4), "abc");
  auto str5 = "abc/xyz";
  EXPECT_EQ(nebula::common::Chars::last(str5), "xyz");
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

struct SimpleB {
  SimpleB() {
    LOG(INFO) << "CONSTRUCT B";
  }
  virtual ~SimpleB() {
    LOG(INFO) << "DESTRUCT B";
  }
};
struct SimpleS : public SimpleB {
  SimpleS() {
    LOG(INFO) << "CONSTRUCT S";
  }
  ~SimpleS() = default;
};

TEST(CommonTest, TestConstructDestructOrder) {
  auto s = std::make_unique<SimpleS>();
  LOG(INFO) << (s != nullptr);
  s = nullptr;
  LOG(INFO) << "destructor already called";
}

TEST(CommonTest, TestFormat) {
  {
    robin_hood::unordered_map<std::string_view, std::string_view> x{ { "p", "place" }, { "x", "something" } };
    auto text = nebula::common::format("go to {p} to do {x}", { { "p", "place" }, { "x", "something" } });
    EXPECT_EQ(text, "go to place to do something");
  }
  {
    auto text = nebula::common::format("{a} - {b} - {c}", { { "a", "ABC" },
                                                            { "b", "OPQ" },
                                                            { "c", "XYZ" } });
    EXPECT_EQ(text, "ABC - OPQ - XYZ");
  }
  {
    auto text = nebula::common::format("{a}", { { "a", "ABC" },
                                                { "b", "OPQ" },
                                                { "c", "XYZ" } });
    EXPECT_EQ(text, "ABC");
  }
  {
    auto text = nebula::common::format("a}{b}}", { { "a", "ABC" },
                                                   { "b", "OPQ" },
                                                   { "c", "XYZ" } });
    EXPECT_EQ(text, "a}OPQ}");
  }
  {
    // token not found exception
    EXPECT_THROW(nebula::common::format("{a} - {b} - {c}", { { "a", "ABC" } }),
                 nebula::common::NebulaException);
  }
}

TEST(CommonTest, TestParams) {
  auto json = "{\"a\":[\"a1\", \"a2\"], \"b\":[\"b1\", \"b2\"], \"c\":\"c1\"}";
  rapidjson::Document cd;
  if (cd.Parse(json).HasParseError()) {
    throw NException(fmt::format("Error parsing params-json: {0}", json));
  }

  // convert this into a param list
  N_ENSURE(cd.IsObject(), "expect params-json as an object.");
  ParamList params(cd);

  // for every single combination, we will use it to format template source to get a new spec source
  auto tmp = "s3://nebula/a={a}/hash/b={b}/c={c}";
  auto p = params.next();
  std::vector<std::string> paths;
  while (p.size() > 0) {
    auto path = nebula::common::format(tmp, p);
    LOG(INFO) << "Get a path: " << path;
    paths.push_back(path);
    p = params.next();
  }
  EXPECT_EQ(paths.size(), 4);
}

TEST(CommonTest, TestJsonBigint) {
  // a big int value that normal JSON parse will lose precison
  auto json = "[161074261581108973]";
  rapidjson::Document cd;
  if (cd.Parse(json).HasParseError()) {
    throw NException(fmt::format("Error parsing params-json: {0}", json));
  }

  EXPECT_TRUE(cd.IsArray());
  auto arr = cd.GetArray();
  EXPECT_EQ(arr.Size(), 1);
  auto& item = *arr.begin();
  EXPECT_EQ(item.GetUint64(), 161074261581108973);
}

TEST(CommonTest, TestRegexMatchExpression) {
  /// HACK! - Replace it!
  std::string_view input = "&&(F:id==C:124))";
  std::regex col_regex("&&\\(F:(\\w+)==C:(\\w+)\\)\\)");
  std::smatch matches;
  std::string str(input.data(), input.size());
  auto result = std::regex_search(str, matches, col_regex);
  EXPECT_TRUE(result);
  EXPECT_EQ(matches.size(), 3);
  EXPECT_EQ(matches[1].str(), "id");
  EXPECT_EQ(matches[2].str(), "124");
}

TEST(CommonTest, TestSparkAlgo) {
  {
    auto v1 = 832251343547708167L;
    auto v2 = 681591862263429002L;
    EXPECT_EQ(nebula::common::Spark::hashLong(v1), -1445328789);
    EXPECT_EQ(nebula::common::Spark::hashLong(v2), -1113418789);
  }
  {
    // all these values shall go to bucket 1211
    auto values = { 681591862263429002,
                    832251343547708167,
                    786511659832270883,
                    651544408499615598,
                    745838525697627629,
                    725220483653562305,
                    582231195484511033,
                    770045373694648418,
                    747316269325404644,
                    350366183419019277,
                    726698227281238570,
                    600527068970671589,
                    727753758444011679,
                    241364998687820316,
                    530087956048807305,
                    571816621346140878,
                    182255253578580115,
                    521432600514974447,
                    666532951009379268,
                    204632514227082448,
                    790381940762119745,
                    443815875686984265,
                    237213242781394825,
                    564498271951689728,
                    639792828221967225,
                    491033303030191704,
                    858147041405117484,
                    795589227831131406,
                    802837208481454588,
                    680958543565688668,
                    384917236810304120,
                    737675751372942133,
                    498984971122258693,
                    85005649125053649,
                    755197568673133221,
                    589901388599953987,
                    193303146414476075,
                    796996602714713481,
                    682436287193562385,
                    755549412394035091,
                    19140504574755010,
                    301952487424792111,
                    523402925351903539,
                    673217981706160373,
                    528399106188516357,
                    710513416120468947,
                    596304944320013714,
                    579486814461677238,
                    643874215384061561,
                    823033038060547738,
                    498914602378208966,
                    832955030989555843,
                    772508279741042960,
                    471893004613846732,
                    837528999361194645,
                    677299368868594400,
                    743094144674742699,
                    631067103943747535,
                    591871713436813803,
                    854628604196187929,
                    439664119780483439,
                    165507492464302378,
                    792141159366606713,
                    787215347273999417,
                    288934269752034499,
                    163396430138968491,
                    282741820264321579,
                    783696910065249066,
                    535647086838854343,
                    782359903925773249,
                    473511485729971255,
                    296252619146534173,
                    519251169445420325,
                    606649149714278906,
                    294563769286148994,
                    294071188076912799,
                    597923425436092365,
                    859554416288700514,
                    527202837537587884,
                    446278781733284443,
                    279997439241415831,
                    181481197392627426,
                    170362935812555911,
                    798403977598369608,
                    203365876831884174,
                    414331371876523231,
                    801218727365426260,
                    688699105425447378,
                    705517235283747054,
                    587016270088617162,
                    792352265599138706,
                    513199457446279707,
                    162129792743769404,
                    627689404223226412,
                    351773558302685279,
                    323133479422274244,
                    822118244386100168,
                    105060741215683792,
                    550776366837013393,
                    46936158524946568 };

    for (auto v : values) {
      auto hash = nebula::common::Spark::hashLong(v);
      auto bucket = nebula::common::Spark::hashBucket(v, 10000);
      LOG(INFO) << "V=" << v
                << ", hash=" << hash
                << ", bucket=" << bucket;
      EXPECT_EQ(1211, bucket);
    }
  }
}

TEST(CommonTest, TestBitsRW) {
  // write 5 bits each time
  constexpr auto BITS = 5;
  constexpr auto BYTES = 1024;
  // max number of value can fit in the slice without extension
  constexpr auto LIMIT = BYTES * 8 / BITS;
  const auto MAX = (size_t)std::pow(2, 5) - 1;
  std::vector<size_t> values;
  values.reserve(LIMIT);

  nebula::common::ExtendableSlice slice(BYTES);
  // test single value
  slice.writeBits(0, BITS, 1);
  EXPECT_EQ(slice.readBits(0, BITS), 1);

  LOG(INFO) << "LIMIT: " << LIMIT;
  // inclusive range [0, MAX]
  auto r = nebula::common::Evidence::rand<size_t>(0, MAX);
  for (auto i = 0; i < LIMIT - 2; ++i) {
    auto v = r();
    values.push_back(v);
    N_ENSURE_LE(v, MAX, "value expected to be in range");
    slice.writeBits(i * BITS, BITS, v);
  }

  // verify slice has no growth
  EXPECT_EQ(slice.size(), BYTES);

  // write one more value, slice will double its capacity
  values.push_back(MAX);
  values.push_back(MAX);
  slice.writeBits((LIMIT - 2) * BITS, BITS, MAX);
  slice.writeBits((LIMIT - 1) * BITS, BITS, MAX);
  EXPECT_EQ(slice.size(), BYTES * 2);

  // verify reading back all values match
  auto size = values.size();
  for (size_t i = 0; i < size; ++i) {
    size_t v = slice.readBits(i * BITS, BITS);
    EXPECT_EQ(v, values.at(i));
  }
  LOG(INFO) << "Values read, write and verified: " << size;
}

TEST(CommonTest, TestBitsOps) {
  nebula::common::Byte::print();

  uint8_t byte;
  // bits: 4
  nebula::common::Byte::write(&byte, 0, 4, 7);
  nebula::common::Byte::write(&byte, 4, 4, 5);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 0, 4), 7);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 4, 4), 5);

  // bits: 3, write 3 values
  nebula::common::Byte::write(&byte, 0, 3, 7);
  nebula::common::Byte::write(&byte, 3, 3, 6);
  nebula::common::Byte::write(&byte, 6, 3, 2);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 0, 3), 7);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 3, 3), 6);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 6, 3), 2);

  // bits: 2, write 4 values
  nebula::common::Byte::write(&byte, 0, 2, 1);
  nebula::common::Byte::write(&byte, 2, 2, 3);
  nebula::common::Byte::write(&byte, 4, 2, 2);
  nebula::common::Byte::write(&byte, 6, 2, 3);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 0, 2), 1);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 2, 2), 3);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 4, 2), 2);
  EXPECT_EQ(nebula::common::Byte::read(&byte, 6, 2), 3);

  // bits: 1, write 8 values
  for (auto i = 0; i < 8; ++i) {
    nebula::common::Byte::write(&byte, i, 1, i % 2);
  }
  for (auto i = 0; i < 8; ++i) {
    EXPECT_EQ(nebula::common::Byte::read(&byte, i, 1), i % 2);
  }
}

TEST(CommonTest, TestFinally) {
  auto x = 0;
  {
    EXPECT_EQ(x, 0);
    nebula::common::Finally exit([&x]() { LOG(INFO) << "3"; x = 0; });
    LOG(INFO) << "1";
    x = 1;
    EXPECT_EQ(x, 1);
    LOG(INFO) << "2";
    x = 2;
    EXPECT_EQ(x, 2);
  }

  EXPECT_EQ(x, 0);
}

TEST(CommonTest, TestStackTree) {
  // run 1K times to measure perf diff between vector and hash set for children
  constexpr auto count = 1000;

  nebula::common::Evidence::Duration dur;
  for (auto i = 0; i < count; ++i) {
    // run the stack tree merge methods
    nebula::common::StackTree<std::string, true, false> stack;
    stack.merge(std::vector<std::string>{ "C", "B", "A" });
    stack.merge(std::vector<std::string>{ "D", "B", "A" });
    stack.merge(std::vector<std::string>{ "X", "A" });
    std::stringstream buffer;
    buffer << stack;

    // hash set doesn't retain an order for children
    EXPECT_EQ(buffer.str(),
              "NODE: d=, c=3, l=0\n"
              "NODE: d=A, c=3, l=1\n"
              "NODE: d=B, c=2, l=2\n"
              "NODE: d=C, c=1, l=3\n"
              "NODE: d=D, c=1, l=3\n"
              "NODE: d=X, c=1, l=2\n");

    // run the stack tree merge methods
    nebula::common::StackTree<std::string, true, false> stack2;
    stack2.merge(std::vector<std::string>{ "C", "B", "A" });
    stack2.merge(std::vector<std::string>{ "E", "B", "A" });
    stack2.merge(std::vector<std::string>{ "Y", "A" });

    // now let's merge two stack tree
    stack.merge(stack2);
    buffer.seekp(0);
    buffer.seekg(0);
    buffer << stack;
    EXPECT_EQ(buffer.str(),
              "NODE: d=, c=6, l=0\n"
              "NODE: d=A, c=6, l=1\n"
              "NODE: d=B, c=4, l=2\n"
              "NODE: d=C, c=2, l=3\n"
              "NODE: d=D, c=1, l=3\n"
              "NODE: d=E, c=1, l=3\n"
              "NODE: d=X, c=1, l=2\n"
              "NODE: d=Y, c=1, l=2\n");
  }

  LOG(INFO) << "Complete 1K rounds of HashFrame: " << dur.elapsedMs();
  dur.reset();

  for (auto i = 0; i < count; ++i) {
    // run the stack tree merge methods
    nebula::common::StackTree<std::string, false, false> stack;
    stack.merge(std::vector<std::string>{ "C", "B", "A" });
    stack.merge(std::vector<std::string>{ "D", "B", "A" });
    stack.merge(std::vector<std::string>{ "X", "A" });
    std::stringstream buffer;
    buffer << stack;

    // hash set doesn't retain an order for children
    EXPECT_EQ(buffer.str(),
              "NODE: d=, c=3, l=0\n"
              "NODE: d=A, c=3, l=1\n"
              "NODE: d=B, c=2, l=2\n"
              "NODE: d=C, c=1, l=3\n"
              "NODE: d=D, c=1, l=3\n"
              "NODE: d=X, c=1, l=2\n");

    // run the stack tree merge methods
    nebula::common::StackTree<std::string, false, false> stack2;
    stack2.merge(std::vector<std::string>{ "C", "B", "A" });
    stack2.merge(std::vector<std::string>{ "E", "B", "A" });
    stack2.merge(std::vector<std::string>{ "Y", "A" });

    // now let's merge two stack tree
    stack.merge(stack2);
    buffer.seekp(0);
    buffer.seekg(0);
    buffer << stack;
    EXPECT_EQ(buffer.str(),
              "NODE: d=, c=6, l=0\n"
              "NODE: d=A, c=6, l=1\n"
              "NODE: d=B, c=4, l=2\n"
              "NODE: d=C, c=2, l=3\n"
              "NODE: d=D, c=1, l=3\n"
              "NODE: d=E, c=1, l=3\n"
              "NODE: d=X, c=1, l=2\n"
              "NODE: d=Y, c=1, l=2\n");
  }

  LOG(INFO) << "Complete 1K rounds of VectorFrame: " << dur.elapsedMs();

  {
    // run the stack tree merge methods
    nebula::common::StackTree<std::string, false, true> stack3;

    std::istringstream s1{ "A\nB\nC" };
    stack3.merge(s1);
    std::istringstream s2{ "A\nB\nD\n" };
    stack3.merge(s2);
    std::istringstream s3{ "A\nX" };
    stack3.merge(s3);

    std::stringstream buffer;
    buffer << stack3;

    // hash set doesn't retain an order for children
    EXPECT_EQ(buffer.str(),
              "NODE: d=, c=3, l=0\n"
              "NODE: d=A, c=3, l=1\n"
              "NODE: d=B, c=2, l=2\n"
              "NODE: d=C, c=1, l=3\n"
              "NODE: d=D, c=1, l=3\n"
              "NODE: d=X, c=1, l=2\n");

    // test json serde
    auto j = stack3.jsonfy();
    LOG(INFO) << j;

    // use the json blob to initialize a new stack tree
    std::string_view sv(j);
    nebula::common::StackTree<std::string, false> stack4(sv);
    auto j2 = stack4.jsonfy();
    EXPECT_EQ(j, j2);
  }
}

TEST(CommonTest, TestIStream) {
  std::string_view view = "abc\nxyz";
  nebula::common::IStream stream(view);
  auto count = 0;
  std::string line;
  while (std::getline(stream, line)) {
    LOG(INFO) << line;
    count++;
  }

  EXPECT_EQ(count, 2);
}

TEST(CommonTest, TestConv) {
  std::string_view view = "abc\nxyz";
  EXPECT_EQ(safe_to<int>(view), 0);

  auto x = 33;
  EXPECT_EQ(safe_to<std::string>(x), "33");
}

static char const* header_name() { return "If-Match"; }
TEST(CommonTest, TestStaticCharPointer) {
  std::string x = header_name();
  x += ":";
  EXPECT_EQ(x, "If-Match:");
}

TEST(CommonTest, TestRapidJsonHandles) {
  // parse empty json string will signal failure
  {
    std::string str;
    rapidjson::Document doc;
    auto& parsed = doc.Parse(str.data(), str.size());
    EXPECT_TRUE(parsed.HasParseError() || doc.IsObject());
  }

  // data writing
  {
    const std::string invalidJson = "{\"v\":Infinity}";
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer,
                      rapidjson::UTF8<>,
                      rapidjson::UTF8<>,
                      rapidjson::CrtAllocator,
                      rapidjson::kWriteNanAndInfFlag>
      json(buffer);
    json.StartObject();
    json.Key("v");

    // will succeed since we specified the flag
    auto dr = json.Double(std::numeric_limits<float>::infinity());
    EXPECT_TRUE(dr);
    json.EndObject();
    auto result = buffer.GetString();
    LOG(INFO) << "result=" << result << "|dr=" << dr;
    EXPECT_EQ(result, invalidJson);
  }

  // sum too many doubles will end up infinitiy?
  {
    float sum = std::numeric_limits<float>::max();
    for (int i = 0; i < 100; ++i) {
      sum += std::numeric_limits<float>::max();
    }
    EXPECT_EQ(std::to_string(sum), "inf");
  }
}

TEST(FormatTest, TestUnformat) {
  std::vector<std::string> tests = {
    "123,456",
    "1,23,45,6",
    "1,2345,6,",
    "1,2,3,4,5,6",
    "12, 3456",
    "12345,6",
    "123456,",
    ", 123456"
  };

  // test string type has no change
  for (size_t i = 0; i < tests.size(); ++i) {
    // make a copy for test
    std::string str(tests.at(i));
    nebula::common::unformat<std::string>(str);
    EXPECT_EQ(str, tests.at(i));
  }

  // test number type has removed comma and space
  for (size_t i = 0; i < tests.size(); ++i) {
    // make a copy for test
    std::string str(tests.at(i));
    nebula::common::unformat<int64_t>(str);
    EXPECT_EQ(str, "123456");
  }

  // test one floating value
  std::string fstr{ "123,456.789" };
  nebula::common::unformat<float>(fstr);
  EXPECT_EQ(fstr, "123456.789");
}

TEST(FormatTest, TestSheetColumnName) {
  for (size_t i = 1; i < 150; ++i) {
    LOG(INFO) << nebula::common::sheetColName(i);
  }

  EXPECT_EQ(nebula::common::sheetColName(1), "A");
  EXPECT_EQ(nebula::common::sheetColName(5), "E");
  EXPECT_EQ(nebula::common::sheetColName(10), "J");
  EXPECT_EQ(nebula::common::sheetColName(20), "T");
  EXPECT_EQ(nebula::common::sheetColName(26), "Z");
  EXPECT_EQ(nebula::common::sheetColName(27), "AA");
  EXPECT_EQ(nebula::common::sheetColName(52), "AZ");
  EXPECT_EQ(nebula::common::sheetColName(77), "BY");
}

} // namespace test
} // namespace common
} // namespace nebula