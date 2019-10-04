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
#include <unordered_map>

#include "common/Evidence.h"
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

private:
  void recordReset(const std::string& addr) {
    auto reported = resets_.find(addr);
    if (reported != resets_.end()) {
      // increment the size
      ++reported->second.second;
    } else {
      resets_.emplace(addr, std::pair{ nebula::common::Evidence::unix_timestamp(), 1 });
    }

    // process all resets
    for (auto itr = resets_.begin(); itr != resets_.end(); ++itr) {
      auto count = itr->second.second;

      // TODO(cao): simple algo for now = if by avg reset in less than 5 minutes
      // mark this node as bad node
      if (count > 3) {
        auto durationSeconds = nebula::common::Evidence::unix_timestamp() - reported->second.first;
        auto avgSeconds = durationSeconds / count;
        if (avgSeconds < 300) {
          LOG(INFO) << "Marking this node as bad since it is reseting every " << avgSeconds;
          nebula::meta::ClusterInfo::singleton().mark(addr);
        } else if (avgSeconds > 3600) {
          LOG(INFO) << "Reactivating this node since it is reseting every " << avgSeconds;
          nebula::meta::ClusterInfo::singleton().mark(addr, nebula::meta::NState::ACTIVE);
        }
      }
    }
  }

private:
  ConnectionPool() = default;
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> connections_;
  //recording resets times, firs time stamp to reset and total reset count
  std::unordered_map<std::string, std::pair<size_t, size_t>> resets_;
};

} // namespace node
} // namespace service
} // namespace nebula
