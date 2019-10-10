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

#include <yaml-cpp/yaml.h>

#include "ClusterInfo.h"
#include "type/Serde.h"

namespace YAML {
template <>
struct convert<nebula::meta::ServerOptions> {
  static Node encode(const nebula::meta::ServerOptions& so) {
    Node node;
    node.push_back(so.anode);
    return node;
  }

  static bool decode(const Node& node, nebula::meta::ServerOptions& so) {
    if (!node.IsMap()) {
      return false;
    }

    so.anode = node["anode"].as<bool>();
    return true;
  }
};
} // namespace YAML

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace meta {

using nebula::meta::Column;
using nebula::type::TypeSerializer;

DataSource asDataSource(const std::string& data) {
  if (data == "custom") {
    return DataSource::Custom;
  }

  if (data == "s3") {
    return DataSource::S3;
  }

  if (data == "kafka") {
    return DataSource::KAFKA;
  }

  throw NException("Misconfigured data source");
}

TimeSpec asTimeSpec(const YAML::Node& node) {
  auto timeType = node["type"].as<std::string>();
  if (timeType == "static") {
    return { TimeType::STATIC, node["value"].as<size_t>(), "", "" };
  }

  if (timeType == "current") {
    return { TimeType::CURRENT, 0, "", "" };
  }

  if (timeType == "column") {
    return {
      TimeType::COLUMN, 0, node["column"].as<std::string>(), node["pattern"].as<std::string>()
    };
  }

  if (timeType == "macro") {
    return { TimeType::MACRO, 0, "", node["pattern"].as<std::string>() };
  }

  throw NException("Misconfigured time spec");
}

Column col(const YAML::Node& settings) {
#define EVAL_SETTING(NAME, VAR, TYPE) \
  const auto& NAME = settings[#NAME]; \
  if (NAME) {                         \
    VAR = NAME.as<TYPE>();            \
  }

  bool bf = false;
  EVAL_SETTING(bloom_filter, bf, bool)

  bool d = false;
  EVAL_SETTING(dict, d, bool)

  std::string dv;
  EVAL_SETTING(default_value, dv, std::string)

  return Column{ bf, d, std::move(dv) };

#undef EVAL_SETTING
}

std::unordered_map<std::string, Column> asColumnProps(const YAML::Node& node) {
  // iterate over each column
  ColumnProps props;
  for (YAML::const_iterator it = node.begin(); it != node.end(); ++it) {
    auto colName = it->first.as<std::string>();
    const auto& settings = it->second;
    props.emplace(colName, col(settings));
  }

  return props;
}

Serde asSerde(const YAML::Node& node) {
  Serde serde;
  if (node) {
    serde.protocol = node["protocol"].as<std::string>();
    auto maps = node["cmap"];
    if (maps) {
      for (YAML::const_iterator it = maps.begin(); it != maps.end(); ++it) {
        serde.cmap.emplace(it->first.as<std::string>(), it->second.as<uint32_t>());
      }
    }
  }

  return serde;
}

void ClusterInfo::load(const std::string& file) {

  YAML::Node config = YAML::LoadFile(file);

  // set the version from the config
  version_ = config["version"].as<std::string>();

  // load server info
  server_ = config["server"].as<ServerOptions>();

  // load all nodes configured for the cluster
  const auto& nodes = config["nodes"];
  NNodeSet nodeSet;
  for (size_t i = 0, size = nodes.size(); i < size; ++i) {
    const auto& node = nodes[i]["node"];

    // if this is an existing node, and we may want to check its state
    NNode nn = { NRole::NODE, node["host"].as<std::string>(), node["port"].as<size_t>() };
    auto existing = nodes_.find(nn);
    if (existing != nodes_.end()) {
      nn.state = existing->state;
    }

    nodeSet.insert(nn);
  }

  // add current server as a node if option says so
  if (server_.anode) {
    nodeSet.emplace(NNode::inproc());
  }

  // swap with new node set
  std::swap(nodes_, nodeSet);

  // load all table specs
  const auto& tables = config["tables"];
  TableSpecSet tableSet;
  for (YAML::const_iterator it = tables.begin(); it != tables.end(); ++it) {
    std::string name = it->first.as<std::string>();

    // table definition
    const auto& td = it->second;
    auto ds = asDataSource(td["data"].as<std::string>());

    // TODO(cao): sorry but we have a hard rule here,
    // every Kafka table will be named as "k.{topic_name}"
    // we may want to turn this into an extra field in table rather than name.
    if (ds == DataSource::KAFKA) {
      // here we want topic name as table name
      name = td["topic"].as<std::string>();
    }

    tableSet.emplace(std::make_shared<TableSpec>(
      name,
      td["max-mb"].as<size_t>(),
      td["max-hr"].as<size_t>(),
      td["schema"].as<std::string>(),
      ds,
      td["loader"].as<std::string>(),
      td["source"].as<std::string>(),
      td["backup"].as<std::string>(),
      td["format"].as<std::string>(),
      asSerde(td["serde"]),
      asColumnProps(td["columns"]),
      asTimeSpec(td["time"])));
  }

  // swap with new table set
  std::swap(tables_, tableSet);
}

void ClusterInfo::update(const NNode& node) {
  std::lock_guard<std::mutex> guard(set_);

  if (nodes_.find(node) != nodes_.end()) {
    return;
  }

  nodes_.insert(node);
}

std::vector<NNode> ClusterInfo::copy() {
  // get a copy of all nodes
  std::lock_guard<std::mutex> guard(set_);
  std::vector<NNode> nodes;
  nodes.reserve(nodes_.size());
  for (auto itr = nodes_.cbegin(); itr != nodes_.cend(); ++itr) {
    nodes.push_back(*itr);
  }

  return nodes;
}

} // namespace meta
} // namespace nebula