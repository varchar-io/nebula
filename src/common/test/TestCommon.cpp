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

#include "common/Errors.h"
#include "common/Evidence.h"
#include "common/Hash.h"
#include "common/Likely.h"
#include "common/Memory.h"

namespace nebula {
namespace common {
namespace test {

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

TEST(CommonTest, TestSliceAndPagedSlice) {
  nebula::common::PagedSlice slice(1024);
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

  // write to position overflow sinle slice - paged slice will auto grow
  slice.write(1050, 1.0);
  EXPECT_EQ(slice.read<double>(1050), 1.0);
  EXPECT_EQ(slice.capacity(), 2048);
}

TEST(CommonTest, TestSliceWrite) {
  nebula::common::PagedSlice slice(1024);

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
      return {};
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

} // namespace test
} // namespace common
} // namespace nebula