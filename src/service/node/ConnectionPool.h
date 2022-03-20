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

#include "common/Evidence.h"
#include "common/Hash.h"
#include "meta/ClusterInfo.h"
#include "meta/NNode.h"

namespace nebula {
namespace service {
namespace node {

/**
 * Create a connection pool to maintain connections with nodes in cluster
 */
class ConnectionPool {
public:
  static std::shared_ptr<ConnectionPool> init() noexcept;

public:
  ConnectionPool(ConnectionPool&) = delete;
  ConnectionPool(ConnectionPool&&) = delete;
  virtual ~ConnectionPool() = default;

  // TODO(cao): we don't have maintainance yet,
  // ideally to have health check peridically and recreate channel when necessary.
  // api to get maintained channel
  std::shared_ptr<grpc::Channel> connection(const std::string&);

  // reset the connection to this node
  void reset(const nebula::meta::NNode&);

  // test if a node is connectable
  inline bool test(const nebula::meta::NNode& node) const noexcept {
    auto channel = this->connect(node.toString());
    return grpc_connectivity_state::GRPC_CHANNEL_READY == channel->GetState(false);
  }

private:
  static const grpc::ChannelArguments& getArgs() {
    // TODO(cao): not clear how to best tune these settings to avoid unstable errors.
    // such as
    //   Received a GOAWAY with error code ENHANCE_YOUR_CALM and debug data equal to "too_many_pings"
    // Simialr issue: https://github.com/grpc/grpc-node/issues/138
    // Commenting out these settings for now.
    // keep alive connection settings
    // chArgs.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    // chArgs.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
    // chArgs.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    // chArgs.SetInt(GRPC_ARG_HTTP2_BDP_PROBE, 1);
    // chArgs.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000);
    // chArgs.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
    // chArgs.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    static grpc::ChannelArguments chArgs;
    chArgs.SetMaxReceiveMessageSize(-1);
    return chArgs;
  }

  // try to connect to the address
  std::shared_ptr<grpc::Channel> connect(const std::string&) const noexcept;

  // record node reset events
  void recordReset(const std::string&);

private:
  ConnectionPool() = default;
  nebula::common::unordered_map<std::string, std::shared_ptr<grpc::Channel>> connections_;
  // recording resets times, firs time stamp to reset and total reset count
  nebula::common::unordered_map<std::string, std::pair<size_t, size_t>> resets_;
};

} // namespace node
} // namespace service
} // namespace nebula
