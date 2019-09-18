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

#include <grpcpp/grpcpp.h>
#include "QueryHandler.h"
#include "meta/TestTable.h"
#include "nebula.grpc.pb.h"

/**
 * A cursor template that help iterating a container.
 * (should we use std::iterator instead?)
 */
namespace nebula {
namespace service {
namespace server {

class V1ServiceImpl final : public V1::Service {
  grpc::Status Tables(grpc::ServerContext*, const ListTables*, TableList*);
  grpc::Status State(grpc::ServerContext*, const TableStateRequest*, TableStateResponse*);
  grpc::Status Query(grpc::ServerContext*, const QueryRequest*, QueryResponse*);

  // query handler to handle all the queries
  QueryHandler handler_;

public:
  V1ServiceImpl() : threadPool_{ std::thread::hardware_concurrency() } {}
  virtual ~V1ServiceImpl() = default;

  folly::ThreadPoolExecutor& pool() {
    return threadPool_;
  }

private:
  grpc::Status replyError(nebula::service::base::ErrorCode, QueryResponse*, size_t) const;
  folly::CPUThreadPoolExecutor threadPool_;
};

} // namespace server
} // namespace service
} // namespace nebula