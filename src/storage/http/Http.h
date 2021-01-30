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

#pragma once

// defines a way to communicate with REST service.
// we are using curl as underlying lib for now.
// ref: examples https://curl.haxx.se/libcurl/c/example.html
//      https://curl.haxx.se/libcurl/c/simple.html
//      https://curl.haxx.se/libcurl/c/https.html
#include <curl/curl.h>
#include <string>

/**
 * An data exchange channel through HTTP.
 * Keep in mind: 
 *  This adds one option of data source coming from HTTP(S) service.
 *  It will follow the way how we consume data from file system or real time data store.
 */
namespace nebula {
namespace storage {
namespace http {

// Define a HTTP service interface
// Not thread-safe, one object intended to be used per thread.
class HttpService {
public:
  explicit HttpService();
  virtual ~HttpService();

public:
  std::string readJson(const std::string&, const std::vector<std::string>&) const;

private:
  CURL* curl_;
};
} // namespace http
} // namespace storage
} // namespace nebula