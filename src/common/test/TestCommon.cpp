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
#include <glog/logging.h>
#include <valarray>
#include "common/Errors.h"
#include "common/Evidence.h"
#include "common/Likely.h"
#include "common/Memory.h"
#include "fmt/format.h"

namespace nebula {
namespace common {
namespace test {

TEST(ErrorTests, Ensures) {
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

TEST(VectorComputing, TestValArray) {
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

TEST(MemoryTest, TestNewDeleteOps) {
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

TEST(MemoryTest, TestSliceAndPagedSlice) {
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

TEST(SliceTest, TestSliceWrite) {
  nebula::common::PagedSlice slice(1024);

  auto value = 1699213050;
  slice.write(0, value);
  EXPECT_EQ(slice.read<int>(0), value);
}

TEST(EvidenceTest, TestTimeParsing) {
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

TEST(EvidenceTest, TestRand) {
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

TEST(StringViewTest, TestStringView) {
  auto str = "abcdefg";
  std::string_view v1(str, 2);
  EXPECT_EQ(v1, "cdefg");
}

} // namespace test
} // namespace common
} // namespace nebula