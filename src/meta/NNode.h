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

#include "Table.h"
#include "meta/Types.h"

/**
 * Define nebula table and system metadata
 * which manages what data segments are loaded in memory for each table
 * This meta data can persist and sync with external DB system such as MYSQL or RocksDB
 * (A KV store is necessary for Nebula to manage all metadata)
 *
 * (Also - Is this responsibility of zookeeper?)
 */
namespace nebula {
namespace meta {

struct NNode {
  // node basics - server and port
  NRole role;
  std::string server;
  size_t port;
  NState state;

  // data size on the node
  size_t size;

  NNode(NRole r, std::string h, size_t p)
    : role{ r }, server{ std::move(h) }, port{ p }, state{ NState::ACTIVE }, size{ 0 } {}

  NNode(const NNode& n)
    : role{ n.role }, server{ n.server }, port{ n.port }, state{ n.state }, size{ n.size } {}

  NNode(NNode&& n)
    : role{ n.role }, server{ std::move(n.server) }, port{ n.port }, state{ n.state }, size{ n.size } {}

  NNode& operator=(const NNode& n) noexcept {
    role = n.role;
    server = n.server;
    port = n.port;
    state = n.state;
    size = n.size;
    return *this;
  }

  NNode& operator=(NNode&& n) noexcept {
    role = n.role;
    server = std::move(n.server);
    port = n.port;
    state = n.state;
    size = n.size;
    return *this;
  }

  inline std::string toString() const {
    return fmt::format("{0}:{1}", server, port);
  }

  inline bool equals(const NNode& other) const {
    if (this == &other) {
      return true;
    }

    return role == other.role && server == other.server && port == other.port;
  }

  inline bool isInProc() const {
    return this->equals(inproc());
  }

  inline bool isLocal() const {
    return this->equals(local());
  }

  inline bool isInvalid() const {
    return this->equals(invalid());
  }

  inline bool isActive() const {
    return state == nebula::meta::NState::ACTIVE;
  }

  static const NNode& local() {
    static const NNode LOCAL = NNode{ NRole::SERVER, "localhost", 9190 };
    return LOCAL;
  }

  static const NNode& inproc() {
    static const NNode INPROC = NNode{ NRole::NODE, "0", 0 };
    return INPROC;
  }

  static const NNode& invalid() {
    static const NNode INVALID = NNode{ NRole::NODE, "invalid", 0 };
    return INVALID;
  }

  MSGPACK_DEFINE(role, server, port, state, size)
}; // namespace meta

struct NodeHash {
public:
  size_t operator()(const NNode& node) const {
    return (std::hash<size_t>()(static_cast<size_t>(node.role)))
           ^ (std::hash<size_t>()(node.port) * 111)
           ^ (std::hash<std::string>()(node.server) * 11111);
  }
};

struct NodeEqual {
public:
  bool operator()(const NNode& node1, const NNode& node2) const {
    // TODO(cao) - verify if there is duplicate nodes
    // while one is using IP and the other using DNS
    return node1.role == node2.role
           && node1.server == node2.server
           && node1.port == node2.port;
  }
};

} // namespace meta
} // namespace nebula