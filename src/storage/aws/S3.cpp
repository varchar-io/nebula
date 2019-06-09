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
#include <aws/s3/model/ListObjectsRequest.h>

#include <glog/logging.h>

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace aws {

std::vector<std::string> S3::list(const std::string& prefix) {
  Aws::S3::S3Client client;
  Aws::S3::Model::ListObjectsRequest listReq;
  listReq.SetBucket(this->bucket_);
  listReq.SetPrefix(prefix);

  // call client
  LOG(INFO) << "call list api: " << prefix;
  auto outcome = client.ListObjects(listReq);
  LOG(INFO) << "client return: ";
  if (outcome.IsSuccess()) {
    LOG(ERROR) << fmt::format("Error listing prefix {0}: {1}", prefix, outcome.GetError().GetMessage());
    return {};
  }

  // translate into lists
  auto result = std::move(outcome.GetResultWithOwnership());
  const auto& cp = result.GetCommonPrefixes();
  const auto size = cp.size();
  LOG(INFO) << "size: " << size;
  std::vector<std::string> objects;
  objects.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    auto x = cp.at(i).GetPrefix();
    LOG(INFO) << x;
    objects.push_back(x);
  }

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