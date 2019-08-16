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

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace meta {

using nebula::type::TypeSerializer;

DataSource asDataSource(const std::string& data) {
  if (data == "custom") {
    return DataSource::Custom;
  }

  if (data == "s3") {
    return DataSource::S3;
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

  throw NException("Misconfigured time spec");
}

void ClusterInfo::load(const std::string& file) {
  YAML::Node config = YAML::LoadFile(file);

  // load all nodes configured for the cluster
  const auto& nodes = config["nodes"];
  nodes_.clear();
  for (size_t i = 0, size = nodes.size(); i < size; ++i) {
    const auto& node = nodes[i]["node"];
    nodes_.emplace(NRole::NODE, node["host"].as<std::string>(), node["port"].as<size_t>());
  }

  // load all table specs
  const auto& tables = config["tables"];
  for (YAML::const_iterator it = tables.begin(); it != tables.end(); ++it) {
    std::string name = it->first.as<std::string>();

    // table definition
    const auto& td = it->second;
    tables_.emplace(name,
                    td["max-mb"].as<size_t>(),
                    td["max-hr"].as<size_t>(),
                    TypeSerializer::from(td["schema"].as<std::string>()),
                    asDataSource(td["data"].as<std::string>()),
                    td["loader"].as<std::string>(),
                    td["source"].as<std::string>(),
                    td["backup"].as<std::string>(),
                    td["format"].as<std::string>(),
                    asTimeSpec(td["time"]));
  }
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