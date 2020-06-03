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

#include "S3.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <cstdio>
#include <fstream>
#include <glog/logging.h>
#include <iostream>
#include <unistd.h>

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace aws {

static constexpr auto S3_LIST_NO_LIMIT = std::numeric_limits<int>::max();

// share the same client across threads
const Aws::S3::S3Client& s3client() {
  static Aws::SDKOptions options;
  static std::atomic<bool> initialized = false;
  if (!initialized) {
    Aws::InitAPI(options);
    initialized = true;
  }

  static Aws::Client::ClientConfiguration conf;
  conf.maxConnections = std::thread::hardware_concurrency();
  // conf.verifySSL = false;
  static const Aws::S3::S3Client S3C{ conf };
  return S3C;
}

std::vector<FileInfo> S3::list(const std::string& prefix) {
  // token for continuation
  Aws::String token;
  std::vector<FileInfo> objects;

  do {
    Aws::S3::Model::ListObjectsV2Request listReq;

    // It is important to specify "delimiter" to return folders only
    // Otherwise the call will return all objects at any level with maximum 1K keys
    listReq.SetBucket(this->bucket_);
    listReq.SetPrefix(prefix);
    listReq.SetMaxKeys(S3_LIST_NO_LIMIT);
    // if token has valid value, set it
    if (token.size() > 0) {
      listReq.SetContinuationToken(token);
    }

    // work on the outcome
    auto outcome = s3client().ListObjectsV2(listReq);
    if (!outcome.IsSuccess()) {
      LOG(ERROR) << fmt::format("Error listing prefix {0}: {1}", prefix, outcome.GetError().GetMessage());
      return {};
    }

    // translate into lists
    auto result = std::move(outcome.GetResultWithOwnership());

    // reset token for next fetch
    token = result.GetIsTruncated() ? result.GetNextContinuationToken() : "";

    // extraction into objects to return
#define EXTRACT_LIST(FETCH, KEY, SIZE, ISD)                     \
  {                                                             \
    const auto& list = result.FETCH();                          \
    for (auto itr = list.cbegin(); itr != list.cend(); ++itr) { \
      objects.emplace_back(ISD, 0, SIZE, itr->KEY(), bucket_);  \
    }                                                           \
  }

    // list all prefix first - folder operation
    EXTRACT_LIST(GetCommonPrefixes, GetPrefix, 0, true)

    // list all objects now - objects
    EXTRACT_LIST(GetContents, GetKey, itr->GetSize(), false)

#undef EXTRACT_LIST
  } while (token.size() > 0);

  return objects;
}

void S3::read(const std::string& prefix, const std::string& key) {
  Aws::S3::Model::GetObjectRequest objReq;
  objReq.SetBucket(this->bucket_);
  objReq.SetKey(key);

  // Get the object
  auto outcome = s3client().GetObject(objReq);
  if (!outcome.IsSuccess()) {
    LOG(ERROR) << fmt::format("Error listing prefix {0}: {1}", prefix, outcome.GetError().GetMessage());
    return;
  }

  // Get an Aws::IOStream reference to the retrieved file
  auto& stream = outcome.GetResultWithOwnership().GetBody();

  // Output the first line of the retrieved text file
  LOG(INFO) << "Beginning of file contents:\n";
  char line[255] = { 0 };
  stream.getline(line, 254);
  LOG(INFO) << line;
}

std::string S3::copy(const std::string& key) {
  std::lock_guard<std::mutex> lock(s3s_);

  // Set up the request
  Aws::S3::Model::GetObjectRequest objReq;
  objReq.SetBucket(this->bucket_);
  objReq.SetKey(key);

  // construct a tmp file name from std io lib
  // local file name tmeplate to copy data into
  char f[] = "/tmp/nebula.s3.XXXXXX";
  int ret = mkstemp(f);
  N_ENSURE(ret != -1, "Failed to create temp file");

  // create an out stream and copy data from S3 stream to it
  // This method will stream data into memory instead of going to disk directly
  // std::ofstream output(tmpFile, std::ios::binary);
  // auto& retrieved = content.GetBody();
  // output << retrieved.rdbuf();
  // if (output.tellp() == 0) {
  //   LOG(WARNING) << "Seen an empty file: " << key;
  //   return {};
  // }

  // set the response to stream into the file
  objReq.SetResponseStreamFactory([f]() {
    return Aws::New<Aws::FStream>("s3", f, std::ios_base::out | std::ios_base::binary);
  });

  // Get the object
  LOG(INFO) << "Download S3 object: bucket=" << this->bucket_ << ", key=" << key << ", to=" << f;
  auto result = s3client().GetObject(objReq);
  if (result.IsSuccess()) {
    // Get an Aws::IOStream reference to the retrieved file
    auto content = std::move(result.GetResultWithOwnership());

    // the object has no data
    if (content.GetContentLength() == 0) {
      LOG(WARNING) << "Seen an empty file: " << key;
      return {};
    }

    // return a copy of file
    return f;
  }

  auto error = result.GetError();
  LOG(ERROR) << "ERROR: " << error;
  return {};
}

} // namespace aws
} // namespace storage
} // namespace nebula