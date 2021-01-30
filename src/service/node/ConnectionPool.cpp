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

#include "ConnectionPool.h"
#include <glog/logging.h>

namespace nebula {
namespace service {
namespace node {

std::shared_ptr<ConnectionPool> ConnectionPool::init() noexcept {
  static const auto inst = std::shared_ptr<ConnectionPool>(new ConnectionPool());
  return inst;
}

// TODO(cao): we don't have maintainance yet,
// ideally to have health check peridically and recreate channel when necessary.
// api to get maintained channel
std::shared_ptr<grpc::Channel> ConnectionPool::connection(const std::string& addr) {
  // lock here for new connection creation?
  auto located = connections_.find(addr);
  if (located != connections_.end()) {
    // do a little maintainance here
    auto channel = located->second;
    auto state = channel->GetState(true);
    if (state != grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN) {
      return channel;
    }

    // will be replaced by channel recreation below
    LOG(INFO) << "Seeing a dead channel to " << addr;
  }

  // all client configurations come to here
  grpc::ChannelArguments chArgs;
  chArgs.SetMaxReceiveMessageSize(-1);

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
  LOG(INFO) << "Creating a channel to " << addr;
  auto channel = grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), chArgs);
  auto state = channel->GetState(true);
  if (state == grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN || state == grpc_connectivity_state::GRPC_CHANNEL_TRANSIENT_FAILURE) {
    // a bad channel when creating
    recordReset(addr);
    return channel;
  }

  connections_[addr] = channel;

  return channel;
}

void ConnectionPool::reset(const nebula::meta::NNode& node) {
  const auto addr = node.toString();
  connections_.erase(addr);
  LOG(INFO) << "Removing a channel to " << addr;
  recordReset(addr);
}

} // namespace node
} // namespace service
} // namespace nebula