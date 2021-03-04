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
#include <google/cloud/storage/client.h>
#include <gtest/gtest.h>

namespace nebula {
namespace common {
namespace test {

TEST(CommonTest, TestGcsQuickStart) {
  // Create a client to communicate with Google Cloud Storage. This client
  // uses the default configuration for authentication and project id.
  const auto bucket_name = "nebula-com";
  google::cloud::StatusOr<google::cloud::storage::Client> client
    = google::cloud::storage::Client::CreateDefaultClient();
  if (!client) {
    LOG(ERROR) << "Failed to create Storage Client, status=" << client.status();
    return;
  }

  auto writer = client->WriteObject(bucket_name, "quickstart2.txt");
  writer << "Hello World!";
  writer.Close();
  if (writer.metadata()) {
    LOG(INFO) << "Successfully created object: " << *writer.metadata();
  } else {
    LOG(ERROR) << "Error creating object: " << writer.metadata().status();
    return;
  }

  auto reader = client->ReadObject(bucket_name, "quickstart2.txt");
  if (!reader) {
    LOG(ERROR) << "Error reading object: " << reader.status();
    return;
  }

  std::string contents{ std::istreambuf_iterator<char>{ reader }, {} };
  LOG(INFO) << contents;
}

} // namespace test
} // namespace common
} // namespace nebula