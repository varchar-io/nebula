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
                         tmpFile);
  // this download should be always true
  EXPECT_TRUE(r);

  // check the file size
  auto info = fs->info(tmpFile);
  EXPECT_EQ(info.size, 476920);
}

TEST(HttpTest, TestReadGoogleSheets) {
  auto url = "https://sheets.googleapis.com/v4/spreadsheets/10FP5MEDG-iGtWO8y4Wd6dyKSHcETg4sKCkBFf8DvqFQ/values:batchGet?majorDimension=COLUMNS&ranges=A%3AZ&key=AIzaSyDdDCIglhCzcSz6lXEoq9qNekfL4C7Jx2A";
  nebula::storage::http::HttpService http;
  std::vector<std::string> headers{
    "Authorization: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjUxMDM2YWYyZDgzOWE4NDJhZjQzY2VjZmJiZDU4YWYxYTc1OGVlYTIiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoiU2hhd24gQ2FvIiwicGljdHVyZSI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hLS9BT2gxNEdnSXZIOTR5MURIa2pzWnNRaVFpQkpUUHdMTVZZT0cweXl3YTNLLVAtcyIsImlzcyI6Imh0dHBzOi8vc2VjdXJldG9rZW4uZ29vZ2xlLmNvbS9uZWJ1bGEtY29tIiwiYXVkIjoibmVidWxhLWNvbSIsImF1dGhfdGltZSI6MTU5ODQ3MTk2NSwidXNlcl9pZCI6Ilp3UVZMdHp3eFNVb3RsRE5vYmlOZmd5bTlIeDIiLCJzdWIiOiJad1FWTHR6d3hTVW90bEROb2JpTmZneW05SHgyIiwiaWF0IjoxNTk4NzY2NDAxLCJleHAiOjE1OTg3NzAwMDEsImVtYWlsIjoiY2FveGh1YUBnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZmlyZWJhc2UiOnsiaWRlbnRpdGllcyI6eyJnb29nbGUuY29tIjpbIjEwODE0MDY3MTQ0NDA4ODI1MDY4MCJdLCJlbWFpbCI6WyJjYW94aHVhQGdtYWlsLmNvbSJdfSwic2lnbl9pbl9wcm92aWRlciI6Imdvb2dsZS5jb20ifX0.dfk0rNdCK8C8UoYGhdUxbeecuDxKDXDfotApvK_W6rySfyhfHu8OadEJAZTq6HbSCWlcWcrGIU-2MAWyOECBjdVu3YsLnNGAXY0_NLNkOFwK1c-WH_dQM6lweKSNT9GpoxymmaIRhIZs0zN1bM8mYMqekTwm8X_tQbO2ttWjazYrlYx0gIgo09TG3Twx0e88j_IyKFAnq7c82_vr09kuoL94G_pUsNQrtmoPWvrBNJdQyl6D34MZN54s8YY-4jkH7xNq235gNojiGcRlPNX14ewspGNzekDIV2GL2JMGVZeXRadBqFPgDM7er_ZDDpv84Df1DVJJ0e5MW-Mpt8j11Q",
    "Accept: application/json"
  };
  auto json = http.readJson(url, headers);
  EXPECT_TRUE(json.size() > 0);
  LOG(INFO) << "Content: " << json;
}

} // namespace test
} // namespace storage
} // namespace nebula