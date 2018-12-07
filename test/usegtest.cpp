#include "folly/Optional.h"
#include "gtest/gtest.h"

TEST(MathTest, TwoPlusTwoEqualsFour) { EXPECT_EQ(4, 2 + 2); }

TEST(MathTest, Divison) { EXPECT_EQ(2, 4 / 2); }

TEST(BetaTest, Great) { EXPECT_GT(2, 1); }

TEST(FollyTest, TestOptional) {
  folly::Optional<uint32_t> opt = folly::none;
  EXPECT_TRUE(!opt);

  opt = 3;
  EXPECT_EQ(opt.value(), 3);
}