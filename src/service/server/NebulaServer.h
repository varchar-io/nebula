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

#include <grpcpp/grpcpp.h>

#include "LoadHandler.h"
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
  grpc::Status Nuclear(grpc::ServerContext*, const EchoRequest*, EchoResponse*);
  grpc::Status Load(grpc::ServerContext*, const LoadRequest*, LoadResponse*);
  grpc::Status Url(grpc::ServerContext*, const UrlData*, UrlData*);
  grpc::Status Ping(grpc::ServerContext*, const ServiceInfo*, PingResponse*);

  // query handler to handle all the queries
  QueryHandler handler_;

public:
  V1ServiceImpl() : threadPool_{ std::thread::hardware_concurrency() } {}
  virtual ~V1ServiceImpl() = default;

  folly::ThreadPoolExecutor& pool() {
    return threadPool_;
  }

  void setShutdownHandler(std::function<void()>&& handler) {
    this->shutdownHandler_ = handler;
  }

private:
  grpc::Status replyError(nebula::service::base::ErrorCode, QueryResponse*, size_t) const;
  folly::CPUThreadPoolExecutor threadPool_;
  LoadHandler loadHandler_;
  std::function<void()> shutdownHandler_;
};

} // namespace server
} // namespace service
} // namespace nebula