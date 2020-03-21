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

#include <folly/compression/Compression.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "common/Memory.h"

/**
 * Test namespace for testing external dependency APIs
 */
namespace nebula {
namespace common {
namespace test {

using folly::io::CodecType;
using folly::io::getCodec;
using nebula::common::CompressionSlice;

TEST(CompressionTest, TestBasicCompressionApi) {
  auto codec = getCodec(CodecType::LZ4);
  auto input = "xyzabcdeasl2ps208uo;1bl/n:ALsjf0w93u2k.QWBQKWDH78yf9283rhkjwnvjksdnaosiuef89qw3yrhoiwefkweFQ";
  LOG(INFO) << "codec: need uncompressed length=" << (codec->needsUncompressedLength());
  auto output = codec->compress(input);
  auto uncompressed = codec->uncompress(output, strlen(input));
  EXPECT_EQ(input, uncompressed);
  for (auto i = 0; i < 1000; ++i) {
    auto output1 = codec->compress(input);
    auto uncompressed1 = codec->uncompress(output, strlen(input));
  }
}

TEST(CompressionTest, TestCompressionSlice) {
  CompressionSlice slice(1024);
  // write 10500 values
  constexpr auto width = sizeof(int64_t);
  constexpr auto total = 10500;
  for (int64_t i = 0; i < total; ++i) {
    slice.write(i * width, i);

    // verify all past values
    for (int64_t x = i; x > 0; x /= 2) {
      EXPECT_EQ(slice.read<int64_t>(x * width), x);
    }
  }

  // flush to finish all writing
  // slice.flush(total * width);

  LOG(INFO) << "raw=" << (total * width) << ", real=" << slice.size();

  // read all values in order and verify their values
  for (int64_t i = 0; i < total; ++i) {
    EXPECT_EQ(slice.read<int64_t>(i * width), i);
  }
}

TEST(CompressionTest, TestStringCompressionSlice) {
  LOG(INFO) << nebula::common::Pool::getDefault().report();
  {
    const char* strings[] = { "abasdfasfasrfqwe", "asdfasfa", "a4po90tu2[093utaldsbvldksvjw-23r08po;jNVSKNVQ;WR02[IALSKDVN" };
    CompressionSlice slice(1024);
    // write 10500 values
    constexpr auto width = sizeof(int64_t);
    constexpr auto total = 10500;
    auto position = 0;
    for (int64_t i = 0; i < total; ++i) {
      auto& s = strings[i % 3];
      position += slice.write(position, s, strlen(s));
    }

    // flush to finish all writing
    // slice.flush(total * width);

    LOG(INFO) << "raw=" << (total * width) << ", real=" << slice.size();

    // read all values in order and verify their values
    position = 0;
    for (int64_t i = 0; i < total; ++i) {
      auto& s = strings[i % 3];
      auto str = slice.read(position, strlen(s));
      EXPECT_EQ(str, s);
      position += strlen(s);
    }
    LOG(INFO) << nebula::common::Pool::getDefault().report();
  }

  LOG(INFO) << nebula::common::Pool::getDefault().report();
}

} // namespace test
} // namespace common
} // namespace nebula