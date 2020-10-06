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
#include <glog/logging.h>

#include "Http.h"

#include "common/Chars.h"
#include "common/Errors.h"
#include "common/Finally.h"

/**
 * An data exchange channel through HTTP.
 * Keep in mind: 
 *  This adds one option of data source coming from HTTP(S) service.
 *  It will follow the way how we consume data from file system or real time data store.
 */
namespace nebula {
namespace storage {
namespace http {

using nebula::common::Chars;

HttpService::HttpService() {
  curl_ = curl_easy_init();
  N_ENSURE_NOT_NULL(curl_, "failed to create curl handle.");

  // skip verifications
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 0L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 0L);
}

HttpService::~HttpService() {
  curl_easy_cleanup(curl_);
  curl_ = nullptr;
}

size_t read(void* data, size_t size, size_t nmemb, std::string* str) {
  size_t len = size * nmemb;
  str->append((char*)data, len);
  return len;
}

// read a given URL to get JSON blob
std::string HttpService::readJson(const std::string& url, const std::vector<std::string>& headers) const {
  static constexpr auto CT_JSON = "application/json";
  static const auto CT_SIZE = std::strlen(CT_JSON);

  // set the URL and perform
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());

  // set callback functio to receive data
  std::string json;
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, read);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &json);

  // if custom headers are present - set them
  struct curl_slist* chunk = NULL;
  nebula::common::Finally releaseChunk([&chunk]() {
    if (chunk) {
      curl_slist_free_all(chunk);
    }
  });

  if (!headers.empty()) {
    for (auto& header : headers) {
      chunk = curl_slist_append(chunk, header.c_str());
    }

    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, chunk);
  }

  // perform the request
  CURLcode res = curl_easy_perform(curl_);

  // check the result code
  if (res != CURLE_OK) {
    LOG(ERROR) << "Failed to read URL: " << url << "; Code=" << res;
    return {};
  }

  // check content type
  char* ct;
  /* ask for the content-type */
  res = curl_easy_getinfo(curl_, CURLINFO_CONTENT_TYPE, &ct);
  if ((CURLE_OK != res) || !ct || !Chars::prefix(ct, CT_SIZE, CT_JSON, CT_SIZE)) {
    LOG(ERROR) << "Expect JSON result. code=" << res
               << ", content-type=" << ct
               << ", content=" << json;
    return {};
  }

  return json;
}

} // namespace http
} // namespace storage
} // namespace nebula