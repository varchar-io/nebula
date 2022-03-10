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

#include <string>

#include "TableSpec.h"

/**
 * Unit test utility to produce a table spec
 * Since it is tedious to repeat the same code
 */
namespace nebula {
namespace meta {

// generate a table spec for unit tests
static inline TableSpecPtr genTableSpec() {
  CsvProps csvProps;
  JsonProps jsonProps;
  ThriftProps thriftProps;
  KafkaSerde kafkaSerde;
  RocksetSerde rocksetSerde;
  TimeSpec timeSpec;
  AccessSpec accSpec;
  ColumnProps colProps;
  BucketInfo bucketInfo = BucketInfo::empty();
  std::unordered_map<std::string, std::string> settings;
  std::map<std::string, std::vector<std::string>> macroValues;
  std::vector<std::string> headers;

  return std::make_shared<TableSpec>(
    "test", 1000, 10, "s3", DataSource::S3,
    "swap", "s3://test", "s3://bak",
    DataFormat::CSV, std::move(csvProps), std::move(jsonProps), std::move(thriftProps),
    std::move(kafkaSerde), std::move(rocksetSerde),
    std::move(colProps), std::move(timeSpec),
    std::move(accSpec), std::move(bucketInfo), std::move(settings),
    std::move(macroValues), std::move(headers), 0);
}

} // namespace meta
} // namespace nebula