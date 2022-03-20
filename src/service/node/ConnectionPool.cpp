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

#include "execution/BlockManager.h"
#include "ingest/SpecRepo.h"

DEFINE_uint64(CONNECTION_WAIT_SECONDS, 3, "Seconds to wait for connection to be done");

namespace nebula {
namespace service {
namespace node {

using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::meta::ClusterInfo;
using nebula::meta::NState;

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
    connections_.erase(addr);
    LOG(INFO) << "Seeing a dead channel to " << addr;
  }

  // create new connection
  LOG(INFO) << "Creating a channel to " << addr;
  auto channel = this->connect(addr);
  auto state = channel->GetState(false);
  if (state == grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN
      || state == grpc_connectivity_state::GRPC_CHANNEL_TRANSIENT_FAILURE) {
    // a bad channel when creating
    recordReset(addr);
    return channel;
  }

  // good connection (ready) or potential good (connecting)
  connections_[addr] = channel;
  return channel;
}

std::shared_ptr<grpc::Channel> ConnectionPool::connect(const std::string& addr) const noexcept {
  auto channel = grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), getArgs());
  auto state = channel->GetState(true);

  // wait the connection to have clear state - ready or failure
  const auto deadline = Evidence::later(FLAGS_CONNECTION_WAIT_SECONDS);
  while (state == grpc_connectivity_state::GRPC_CHANNEL_IDLE
         || state == grpc_connectivity_state::GRPC_CHANNEL_CONNECTING) {
    if (!channel->WaitForStateChange(state, deadline)) {
      break;
    }
  }

  return channel;
}

void ConnectionPool::reset(const nebula::meta::NNode& node) {
  const auto addr = node.toString();
  connections_.erase(addr);
  LOG(INFO) << "Removing a channel to " << addr;
  recordReset(addr);
}

void ConnectionPool::recordReset(const std::string& addr) {
  auto reported = resets_.find(addr);
  if (reported != resets_.end()) {
    // increment the size
    ++reported->second.second;
  } else {
    resets_.emplace(addr, std::pair{ Evidence::unix_timestamp(), 1 });
  }

  // process all resets
  for (auto itr = resets_.begin(); itr != resets_.end(); ++itr) {
    auto count = itr->second.second;

    // TODO(cao): simple algo for now = if by avg reset in less than 5 minutes
    // mark this node as bad node
    if (count > 3) {
      auto& ci = ClusterInfo::singleton();
      auto durationSeconds = Evidence::unix_timestamp() - reported->second.first;
      auto avgSeconds = durationSeconds / count;
      if (avgSeconds < 300) {
        LOG(INFO) << "Marking this node as bad since it is reseting every " << avgSeconds;
        ci.mark(addr);

        // remove all data state from this address
        BlockManager::init()->removeNode(addr);
        nebula::ingest::SpecRepo::singleton().lost(addr);
      } else if (avgSeconds > 3600) {
        LOG(INFO) << "Reactivating this node since it is reseting every " << avgSeconds;
        ci.mark(addr, NState::ACTIVE);
      }
    }
  }
}

} // namespace node
} // namespace service
} // namespace nebula