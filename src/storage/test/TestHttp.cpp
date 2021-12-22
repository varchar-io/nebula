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

#include "storage/NFS.h"
#include "storage/http/Http.h"

namespace nebula {
namespace storage {
namespace test {
using nebula::storage::http::HttpService;

TEST(HttpTest, TestBasicRead) {
  auto url = "https://api.github.com/";
  nebula::storage::http::HttpService http;
  auto json = http.readJson(url, { "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36" });
  EXPECT_TRUE(json.size() > 0);
  LOG(INFO) << "Content: " << json;
}

TEST(HttpTest, TestBasicDownload) {
  auto url = "https://user-images.githubusercontent.com/26632293/103615010-a998c600-4ede-11eb-8b55-ee5dcd4e4026.png";
  nebula::storage::http::HttpService http;
  auto fs = nebula::storage::makeFS("local");
  auto tmpFile = fs->temp();
  bool r = http.download(url,
                         { "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36" },
                         "",
                         tmpFile);
  // this download should be always true
  EXPECT_TRUE(r);

  // check the file size
  auto info = fs->info(tmpFile);
  EXPECT_EQ(info.size, 476920);
}

// disable this test and remove the api key and access token from public source.
TEST(HttpTest, DISABLED_TestReadGoogleSheets) {
  auto url = "https://sheets.googleapis.com/v4/spreadsheets/10FP5MEDG-iGtWO8y4Wd6dyKSHcETg4sKCkBFf8DvqFQ/values:batchGet?majorDimension=COLUMNS&ranges=A%3AZ&key=<api_key>";
  nebula::storage::http::HttpService http;
  std::vector<std::string> headers{
    "Authorization: Bearer <access token>",
    "Accept: application/json"
  };
  auto json = http.readJson(url, headers);
  EXPECT_TRUE(json.size() > 0);
  LOG(INFO) << "Content: " << json;
}

} // namespace test
} // namespace storage
} // namespace nebula