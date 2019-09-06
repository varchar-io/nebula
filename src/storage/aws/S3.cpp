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
#include <glog/logging.h>

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace aws {

std::vector<FileInfo> S3::list(const std::string& prefix) {
  Aws::S3::S3Client client;
  Aws::S3::Model::ListObjectsV2Request listReq;

  // It is important to specify "delimiter" to return folders only
  // Otherwise the call will return all objects at any level with maximum 1K keys
  listReq.SetBucket(this->bucket_);
  listReq.SetPrefix(prefix);
  auto outcome = client.ListObjectsV2(listReq);
  if (!outcome.IsSuccess()) {
    LOG(ERROR) << fmt::format("Error listing prefix {0}: {1}", prefix, outcome.GetError().GetMessage());
    return {};
  }

  // translate into lists
  auto result = std::move(outcome.GetResultWithOwnership());
  std::vector<FileInfo> objects;

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

  return objects;
}

void S3::read(const std::string& prefix, const std::string& key) {
  Aws::S3::S3Client client;
  Aws::S3::Model::GetObjectRequest objReq;
  objReq.SetBucket(this->bucket_);
  objReq.SetKey(key);

  // Get the object
  auto outcome = client.GetObject(objReq);
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

} // namespace aws
} // namespace storage
} // namespace nebula