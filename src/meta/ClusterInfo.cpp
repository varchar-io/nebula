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
struct convert<nebula::meta::MetaConf> {
  static bool decode(const Node& node, nebula::meta::MetaConf& conf) {
    if (!node.IsMap()) {
      return false;
    }

    auto db = node["db"].as<std::string>();
    if (db == "native") {
      conf.type = nebula::meta::DBType::NATIVE;
    }

    conf.store = node["store"].as<std::string>();
    return true;
  }
};
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
    so.authRequired = node["auth"].as<bool>();
    // parse meta conf if presents
    so.metaConf = node["meta"].as<nebula::meta::MetaConf>();

    // populate discovery method
    auto discovery = node["discovery"];
    if (discovery) {
      auto method = discovery["method"].as<std::string>();
      if (method == "config") {
        so.discovery = nebula::meta::Discovery::CONFIG;
      } else if (method == "service") {
        so.discovery = nebula::meta::Discovery::SERVICE;
      } else {
        throw NException(fmt::format("not supported discovery method: {0}.", method));
      }
    }

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

using nebula::common::Evidence;
using nebula::common::unordered_map;
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

  if (data == "local") {
    return DataSource::LOCAL;
  }

  throw NException("Misconfigured data source");
}

AccessType asAccessType(const std::string& name) {
  if (name == "read") return AccessType::READ;
  if (name == "aggregation") return AccessType::AGGREGATION;
  if (name == "write") return AccessType::WRITE;
  return AccessType::UNKNOWN;
}

ActionType asActionType(const YAML::Node& node) {
  if (node) {
    auto action = node.as<std::string>();
    if (action == "deny") return ActionType::DENY;
    if (action == "mask") return ActionType::MASK;
  }

  // any mis-configured action will grant as pass
  return ActionType::PASS;
}

// read defined access rules from table config
// example:
// access:
//    read:
//      groups:
//      action:
std::vector<AccessRule> asAccessRules(const YAML::Node& node) {
  if (node) {
    std::vector<AccessRule> rules;
    rules.reserve(3);
    // for all access type
    for (YAML::const_iterator it = node.begin(); it != node.end(); ++it) {
      auto atype = it->first.as<std::string>();
      const auto& settings = it->second;
      const auto& gtag = settings["groups"];
      std::vector<std::string> groups;
      if (gtag) {
        groups = gtag.as<std::vector<std::string>>();
      }
      const auto& atag = settings["action"];
      rules.emplace_back(asAccessType(atype), groups, asActionType(atag));
    }

    return rules;
  }

  // no rules defined
  return {};
}

// read flat single dimensional key-value list
// example:
// settings:
//    k1:v1
//    ...
//    kn:vn
std::unordered_map<std::string, std::string> asSettings(const YAML::Node& node) {
  if (node) {
    std::unordered_map<std::string, std::string> settings;
    // for all access type
    for (YAML::const_iterator it = node.begin(); it != node.end(); ++it) {
      settings.emplace(it->first.as<std::string>(), it->second.as<std::string>());
    }

    return settings;
  }

  // no rules defined
  return std::unordered_map<std::string, std::string>();
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

  if (timeType == "provided") {
    return { TimeType::PROVIDED, 0, "", "" };
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

  bool c = false;
  EVAL_SETTING(compress, c, bool)

  std::string dv;
  EVAL_SETTING(default_value, dv, std::string)

  // if access spec defined
  const auto& access = settings["access"];
  AccessSpec as;
  if (access) {
    as = asAccessRules(access);
  }

  // if partition spec is defined
  const auto& partition = settings["partition"];
  PartitionInfo pi;
  if (partition) {
    const auto& values = partition["values"];
    N_ENSURE(values, "values must be defined");
    pi.values = values.as<std::vector<std::string>>();

    // by default chunk set as 1, overwrite by config
    const auto& chunk = partition["chunk"];
    pi.chunk = chunk ? chunk.as<size_t>() : 1;
  }

  return Column{ bf, d, c, std::move(dv), std::move(as), std::move(pi) };

#undef EVAL_SETTING
}

// read column properties definition from table config
unordered_map<std::string, Column> asColumnProps(const YAML::Node& node) {
  // iterate over each column
  ColumnProps props;
  for (YAML::const_iterator it = node.begin(); it != node.end(); ++it) {
    auto colName = it->first.as<std::string>();
    const auto& settings = it->second;
    props.emplace(colName, col(settings));
  }

  return props;
}

KafkaSerde asSerde(const YAML::Node& node) {
  KafkaSerde serde;
  if (node) {
    serde.protocol = node["protocol"].as<std::string>();

    // kafka topic retention time
    auto retention = node["topic-retention"];
    if (retention) {
      serde.retention = retention.as<uint64_t>();
    }
    auto size = node["size"];
    if (size) {
      serde.size = size.as<uint64_t>();
    }

    auto maps = node["cmap"];
    if (maps) {
      for (YAML::const_iterator it = maps.begin(); it != maps.end(); ++it) {
        serde.cmap.emplace(it->first.as<std::string>(), it->second.as<uint32_t>());
      }
    }
  }

  return serde;
}

/** [example]
    bucket:
      size: 10000
      column: pid
*/
BucketInfo asBucketInfo(const YAML::Node& node) {
  if (node) {
    auto count = node["size"].as<size_t>();
    auto column = node["column"].as<std::string>();
    return BucketInfo{ count, column };
  }

  // no bucket info
  return BucketInfo::empty();
}

void ClusterInfo::load(const std::string& file, CreateMetaDB createDb) {

  YAML::Node config = YAML::LoadFile(file);

  // total top level section supported
  size_t topLevels = 0;

  // set the version from the config
  version_ = config["version"].as<std::string>();
  topLevels++;

  // load server info
  server_ = config["server"].as<ServerOptions>();
  topLevels++;

  // if we need to create metadb, then do it
  if (!db_) {
    db_ = createDb(server_.metaConf);
  }

  // load all nodes configured for the cluster
  topLevels++;
  if (server_.discovery == Discovery::CONFIG) {
    NNodeSet nodeSet;
    const auto& nodes = config["nodes"];

    for (size_t i = 0, size = nodes.size(); i < size; ++i) {
      const auto& node = nodes[i]["node"];
      nodeSet.emplace(NRole::NODE, node["host"].as<std::string>(), node["port"].as<size_t>());
    }

    // add current server as a node if option says so
    if (server_.anode) {
      nodeSet.emplace(NNode::inproc());
    }

    // replace the default node manager use pre-configured one
    this->nodeManager_ = NodeManager::create(std::move(nodeSet));
  }

  // load all table specs
  const auto& tables = config["tables"];
  topLevels++;
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

    // hour table level retention.max-hr as single way to ingest and evict data
    const auto retention = td["retention"];

    // max-hr could be fractional value to help us get granularity to seconds
    tableSet.emplace(std::make_shared<TableSpec>(
      name,
      retention["max-mb"].as<size_t>(),
      retention["max-hr"].as<double>() * Evidence::HOUR_SECONDS,
      td["schema"].as<std::string>(),
      ds,
      td["loader"].as<std::string>(),
      td["source"].as<std::string>(),
      td["backup"].as<std::string>(),
      td["format"].as<std::string>(),
      asSerde(td["serde"]),
      asColumnProps(td["columns"]),
      asTimeSpec(td["time"]),
      asAccessRules(td["access"]),
      asBucketInfo(td["bucket"]),
      asSettings(td["settings"])));
  }

  // swap with new table set
  std::swap(tables_, tableSet);

  // if user mistakenly config things at top level
  // (I made mistake to place a new table the same level as "tables")
  // let's fail this case as mis-config [version, server, nodes, tables]
  if (config.size() > topLevels) {
    throw NException("Un-recoganized config at the top level");
  }
}

} // namespace meta
} // namespace nebula