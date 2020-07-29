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
#pragma once
// define RAPIDJSON to have stdstring
#ifndef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#endif

#include "api/dsl/Query.h"
#include "common/Task.h"
#include "ingest/IngestSpec.h"
#include "memory/keyed/FlatBuffer.h"
#include "node/node.grpc.fb.h"
#include "node/node_generated.h"
#include "surface/DataSurface.h"
#include "type/Type.h"

/**
 * Define some basic sharable proerpties for nebula service
 */
namespace nebula {
namespace service {
namespace base {

// define all service ERROR code = 0 reserved for NO_ERROR
// And this error code table can be look up
enum ErrorCode {
  NONE = 0,
  MISSING_TABLE = 1,
  MISSING_TIME_RANGE = 2,
  MISSING_OUTPUT_FIELDS = 3,
  FAIL_BUILD_QUERY = 4,
  FAIL_COMPILE_QUERY = 5,
  FAIL_EXECUTE_QUERY = 6,
  AUTH_REQUIRED = 7,
  PERMISSION_REQUIRED = 8
};

template <ErrorCode E>
struct ErrorTraits {};

template <>
struct ErrorTraits<ErrorCode::NONE> {
  static constexpr auto MESSAGE = "";
};

template <>
struct ErrorTraits<ErrorCode::MISSING_TABLE> {
  static constexpr auto MESSAGE = "Table Not Set";
};

template <>
struct ErrorTraits<ErrorCode::MISSING_TIME_RANGE> {
  static constexpr auto MESSAGE = "Time Range Not Set";
};

template <>
struct ErrorTraits<ErrorCode::MISSING_OUTPUT_FIELDS> {
  static constexpr auto MESSAGE = "No Dimension Or Metric Set";
};

template <>
struct ErrorTraits<ErrorCode::FAIL_BUILD_QUERY> {
  static constexpr auto MESSAGE = "Fail To Build Query";
};

template <>
struct ErrorTraits<ErrorCode::FAIL_COMPILE_QUERY> {
  static constexpr auto MESSAGE = "Fail To Compile Query Plan";
};

template <>
struct ErrorTraits<ErrorCode::FAIL_EXECUTE_QUERY> {
  static constexpr auto MESSAGE = "Fail To Execute Query";
};

template <>
struct ErrorTraits<ErrorCode::AUTH_REQUIRED> {
  static constexpr auto MESSAGE = "User Authentication Required To Execute";
};

template <>
struct ErrorTraits<ErrorCode::PERMISSION_REQUIRED> {
  static constexpr auto MESSAGE = "User Has No Permission To Execute";
};

class ServiceProperties final {
public:
  // nebula server listening port
  static constexpr auto PORT = 9190;

  // nebula node server listening port
  static constexpr auto NPORT = 9199;

  // default window size is 60 seconds aka 1 minute
  static constexpr auto DEFAULT_WINDOW_SIZE = 60;

  // default top size to 100 - show top 100 records
  static constexpr auto DEFAULT_TOP_SIZE = 100;

  // get error message from error code
  static const std::string errorMessage(ErrorCode);

  // jsonify a row set of data with given schema
  static const std::string jsonify(const nebula::surface::RowCursorPtr, const nebula::type::Schema);
};

/**
 * A query serializer and deserializer to transmit a query between nodes in flatbuffers
 */
class QuerySerde {
public:
  static flatbuffers::grpc::Message<QueryPlan> serialize(const nebula::api::dsl::Query&, const std::string&, const nebula::execution::QueryWindow&);
  static nebula::api::dsl::Query deserialize(const std::shared_ptr<nebula::meta::MetaService>, const flatbuffers::grpc::Message<QueryPlan>*);
  static std::unique_ptr<nebula::execution::ExecutionPlan> from(nebula::api::dsl::Query&, size_t, size_t);
};

/**
 * A batch serde to transmit a batch between nodes in fb format w/ zero-copy.
 */
class BatchSerde {
public:
  static flatbuffers::grpc::Message<BatchRows> serialize(const nebula::memory::keyed::FlatBuffer&);
  static nebula::surface::RowCursorPtr deserialize(const flatbuffers::grpc::Message<BatchRows>*, const nebula::surface::eval::Fields&);
};

/**
 * A task serde to transmit a task between server and node through flatbuffers
 */
class TaskSerde {
public:
  // we have pair of methods for each task type
  static flatbuffers::grpc::Message<TaskSpec> serialize(const nebula::common::Task&);
  static nebula::common::Task deserialize(const flatbuffers::grpc::Message<TaskSpec>*);
};

} // namespace base
} // namespace service
} // namespace nebula