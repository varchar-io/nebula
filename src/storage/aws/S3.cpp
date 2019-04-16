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
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

/**
 * A wrapper for interacting with AWS / S3
 */
namespace nebula {
namespace storage {
namespace aws {
void S3::read() {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  {
    // snippet-start:[s3.cpp.get_object.code]
    // Assign these values before running the program
    const Aws::String bucket_name = "BUCKET_NAME";
    const Aws::String object_name = "OBJECT_NAME"; // For demo, set to a text file

    // Set up the request
    Aws::S3::S3Client s3_client;
    Aws::S3::Model::GetObjectRequest object_request;
    object_request.SetBucket(bucket_name);
    object_request.SetKey(object_name);

    // Get the object
    auto get_object_outcome = s3_client.GetObject(object_request);
    if (get_object_outcome.IsSuccess()) {
      // Get an Aws::IOStream reference to the retrieved file
      auto& retrieved_file = get_object_outcome.GetResultWithOwnership().GetBody();

      // Output the first line of the retrieved text file
      std::cout << "Beginning of file contents:\n";
      char file_data[255] = { 0 };
      retrieved_file.getline(file_data, 254);
      std::cout << file_data << std::endl;
    } else {
      auto error = get_object_outcome.GetError();
      std::cout << "ERROR: " << error.GetExceptionName() << ": "
                << error.GetMessage() << std::endl;
    }
    // snippet-end:[s3.cpp.get_object.code]
  }
  Aws::ShutdownAPI(options);
}
} // namespace aws
} // namespace storage
} // namespace nebula