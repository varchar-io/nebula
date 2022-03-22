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

#include "common/Spark.h"
#include "meta/TableSpec.h"

namespace nebula {
namespace meta {

class BucketHelper {
  // macro value
  static constexpr auto BUCKET = "bucket";

public:
  static void populate(const nebula::meta::TableSpecPtr tbSpec, nebula::common::MapKV params) {
    auto bucketCount = tbSpec->bucketInfo.count;
    if (params.find(BUCKET) == params.end() && bucketCount > 0) {
      // TODO(cao) - short term dependency, if Spark changes its hash algo, this will be broken
      auto bcValue = folly::to<size_t>(params.at(tbSpec->bucketInfo.bucketColumn));
      auto bucket = std::to_string(nebula::common::Spark::hashBucket(bcValue, bucketCount));

      // TODO(cao) - ugly code, how can we avoid this?
      auto width = std::log10(bucketCount) + 1;
      bucket = std::string((width - bucket.size()), '0').append(bucket);
      params.emplace(BUCKET, bucket);
    }
  }
};

} // namespace meta
} // namespace nebula