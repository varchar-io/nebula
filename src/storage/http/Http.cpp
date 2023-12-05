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
#include <glog/logging.h>
#include <stdio.h>

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

#define SET_HTTP_HEADERS                                \
  struct curl_slist* chunk = NULL;                      \
  nebula::common::Finally releaseChunk([&chunk]() {     \
    if (chunk) {                                        \
      curl_slist_free_all(chunk);                       \
    }                                                   \
  });                                                   \
                                                        \
  if (!headers.empty()) {                               \
    for (auto& header : headers) {                      \
      chunk = curl_slist_append(chunk, header.c_str()); \
    }                                                   \
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, chunk); \
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
  SET_HTTP_HEADERS

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

// buffering from curl and writing into the file (replace with ostream?)
static size_t write(void* ptr, size_t size, size_t nmemb, void* stream) {
  size_t written = fwrite(ptr, size, nmemb, (FILE*)stream);
  return written;
}

// refer: https://curl.se/libcurl/c/curl_easy_getinfo.html
void diagnoseInfo(CURL* curl) {
#define EXTRACT(T, N, V) \
  T N = -1;              \
  curl_easy_getinfo(curl, V, &N);

  EXTRACT(long, response_code, CURLINFO_RESPONSE_CODE)
  EXTRACT(long, err_no, CURLINFO_OS_ERRNO)

  EXTRACT(double, total_time, CURLINFO_TOTAL_TIME)
  EXTRACT(double, dns_time, CURLINFO_NAMELOOKUP_TIME)
  EXTRACT(double, connect_time, CURLINFO_CONNECT_TIME)
  EXTRACT(double, appcon_time, CURLINFO_APPCONNECT_TIME)

  EXTRACT(double, down_size, CURLINFO_SIZE_DOWNLOAD)
  EXTRACT(long, num_connects, CURLINFO_NUM_CONNECTS)
  EXTRACT(long, ssl_vr, CURLINFO_SSL_VERIFYRESULT)

#undef EXTRACT

  char* url = NULL;
  curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &url);

  // format and print as one line info
  LOG(INFO) << "[Curl-Info]: response code=" << response_code
            << ", error number=" << err_no
            << ", time=(total=" << total_time
            << ", dns=" << dns_time
            << ", conn=" << connect_time
            << ", app-conn=" << appcon_time << ")"
            << ", num-conn=" << num_connects
            << ", ssl-verify=" << ssl_vr
            << ", download-size=" << down_size
            << ", url=" << url;
}

// follow https://curl.se/libcurl/c/url2file.html
bool HttpService::download(const std::string& url,
                           const std::vector<std::string>& headers,
                           const std::string_view post,
                           const std::string& local) const {
  // set the URL and perform
  LOG(INFO) << "Downloading from " << url << " to " << local;
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());

  // if custom headers are present - set them
  SET_HTTP_HEADERS

  // if we have data to post, then post it
  if (post.size() > 0) {
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, post.size());
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, post.data());
  }

  // no need progress
  curl_easy_setopt(curl_, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, write);
  // open the file for writing
  FILE* pagefile = fopen(local.c_str(), "wb");
  if (pagefile) {
    nebula::common::Finally closeFile([&pagefile]() {
      fclose(pagefile);
    });
    // start to write data
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, pagefile);

    // perform the download action
    CURLcode res = curl_easy_perform(curl_);
    // check the result code
    if (res != CURLE_OK) {
      LOG(ERROR) << "Failed to read URL: " << url << "; Code=" << res;
      // print some diagnostic info for why we get this error
      diagnoseInfo(curl_);
      return false;
    }

    // check download information
    double size = 0.0;
    res = curl_easy_getinfo(curl_, CURLINFO_SIZE_DOWNLOAD, &size);
    LOG(INFO) << "Downloaded " << size << " bytes from " << url;
    return true;
  }

  // operation system error
  LOG(ERROR) << "Can't create file: " << local;
  return false;
}

#undef SET_HTTP_HEADERS

} // namespace http
} // namespace storage
} // namespace nebula