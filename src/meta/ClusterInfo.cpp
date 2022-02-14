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

#include "ClusterInfo.h"

#include "type/Serde.h"

namespace YAML {
template <>
struct convert<nebula::meta::MetaConf> {
  static bool decode(const Node& node, nebula::meta::MetaConf& conf) {
    if (!node.IsMap()) {
      return false;
    }

    auto nd = node["db"];
    std::string db{ "none" };
    if (nd) {
      db = nd.as<std::string>();
    }

    if (db == "native") {
      conf.type = nebula::meta::DBType::NATIVE;
    } else if (db == "none") {
      conf.type = nebula::meta::DBType::NONE;
    }

    auto ns = node["store"];
    if (ns) {
      conf.store = ns.as<std::string>();
    }
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
using nebula::common::unordered_set;
using nebula::type::Settings;
using nebula::type::TypeSerializer;

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
Settings asSettings(const YAML::Node& node) {
  Settings settings;
  if (node) {
    // for all access type
    for (YAML::const_iterator it = node.begin(); it != node.end(); ++it) {
      settings.emplace(it->first.as<std::string>(), it->second.as<std::string>());
    }
  }

  return settings;
}

TimeSpec asTimeSpec(const YAML::Node& node) {
  TimeSpec spec;
  auto strType = node["type"].as<std::string>();
  spec.type = TimeTypeUtils::from(strType);

  // handle each time type
  switch (spec.type) {
  case TimeType::STATIC:
    spec.unixTimeValue = node["value"].as<size_t>();
    break;
  case TimeType::COLUMN:
    spec.column = node["column"].as<std::string>();
    // "%m/%d/%Y %H:%M:%S"
    spec.pattern = node["pattern"].as<std::string>();
    break;
  case TimeType::MACRO:
    // "daily", "hourly", ...
    spec.pattern = node["pattern"].as<std::string>();
    break;
  case TimeType::PROVIDED:
  case TimeType::CURRENT: break;
  default:
    throw NException(fmt::format("Misconfigured time type: {0}", strType));
  }

  return spec;
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

  std::string fm;
  EVAL_SETTING(from_macro, fm, std::string)

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

  return Column{ bf, d, c, std::move(dv), std::move(fm), std::move(as), std::move(pi) };

#undef EVAL_SETTING
}

// read column properties definition from table config
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

KafkaSerde asKafka(const YAML::Node& node) {
  KafkaSerde kafka;
  if (node) {
    // kafka topic retention time
    auto retention = node["topic-retention"];
    if (retention) {
      kafka.retention = retention.as<uint64_t>();
    }

    auto size = node["size"];
    if (size) {
      kafka.size = size.as<uint64_t>();
    }

    // topic is a must to have
    kafka.topic = node["topic"].as<std::string>();
  }

  return kafka;
}

RocksetSerde asRockset(const YAML::Node& node) {
  RocksetSerde rockset;
  if (node) {
    // kafka topic retention time
    auto apiKey = node["apiKey"];
    if (apiKey) {
      rockset.apiKey = apiKey.as<std::string>();
    }

    auto interval = node["interval"];
    auto v = 0;
    if (interval) {
      v = interval.as<uint32_t>();
    }

    // interval should be minutes [1, 2, 5, 10, 15, 20, 30] from settings
    // it doesn't make sense to have interval larger than 1-hour
    if (v == 0) {
      v = 1;
    } else if (v > 2 && v < 5) {
      v = 5;
    } else if (v > 5 && v < 10) {
      v = 10;
    } else if (v > 10 && v < 15) {
      v = 15;
    } else if (v > 15 && v < 20) {
      v = 20;
    } else if (v > 20 && v < 30) {
      v = 30;
    } else if (v > 30) {
      v = 30;
    }

    // the value is guranteed to fall the boundary within any hour
    rockset.interval = v * 60;
  }

  return rockset;
}

// support top-level macro definitions for each table spec
// such as
//  #   macros:
//  #     part: ["macro-part1", "macro-part2"]
//  or differnet ways for list value
//  #   macros:
//  #     part:
//  #       - "macro-part1"
//  #       - "macro-part2"
std::map<std::string, std::vector<std::string>> asMacroValues(const YAML::Node& node) {
  std::map<std::string, std::vector<std::string>> macroValues;
  if (node && node.IsMap()) {
    for (YAML::const_iterator macro_it = node.begin(); macro_it != node.end(); ++macro_it) {
      if (!macro_it->second.IsSequence()) {
        continue;
      }
      const auto macroName = macro_it->first.as<std::string>();
      if (macroValues.find(macroName) != macroValues.end()) {
        LOG(WARNING) << "Skip the same macro name already defined: " << macroName;
        continue;
      }
      std::vector<std::string> valuesForMacro;
      for (YAML::const_iterator value_it = macro_it->second.begin(); value_it != macro_it->second.end(); ++value_it) {
        valuesForMacro.push_back(value_it->as<std::string>());
      }
      macroValues.emplace(macroName, valuesForMacro);
    }
  }
  return macroValues;
}

// allow users to specify service headers (mostly http)
// for example:
//  #   headers: ['abc', "xyz"]
// or
//  #   headers:
//  #     - "abc"
//  #     - "xyz"
std::vector<std::string> asHeaders(const YAML::Node& node) {
  std::vector<std::string> headers;
  if (node) {
    // for all access type
    for (YAML::const_iterator it = node.begin(); it != node.end(); ++it) {
      headers.emplace_back(it->as<std::string>());
    }
  }

  return headers;
}

CsvProps asCsvProps(const YAML::Node& node) {
  CsvProps csv;
  if (node) {
    csv.hasHeader = node["hasHeader"].as<bool>();
    csv.hasMeta = node["hasMeta"].as<bool>();
    csv.delimiter = node["delimiter"].as<std::string>();
  }
  return csv;
}

JsonProps asJsonProps(const YAML::Node& node) {
  JsonProps json;
  if (node) {
    json.rowsField = node["rowsField"].as<std::string>();
    auto maps = node["columnsMap"];
    if (maps) {
      for (YAML::const_iterator it = maps.begin(); it != maps.end(); ++it) {
        json.columnsMap.emplace(it->first.as<std::string>(), it->second.as<std::string>());
      }
    }
  }
  return json;
}

ThriftProps asThriftProps(const YAML::Node& node) {
  ThriftProps thrift;
  if (node) {
    thrift.protocol = node["protocol"].as<std::string>();
    auto maps = node["columnsMap"];
    if (maps) {
      for (YAML::const_iterator it = maps.begin(); it != maps.end(); ++it) {
        thrift.columnsMap.emplace(it->first.as<std::string>(), it->second.as<uint32_t>());
      }
    }
  }

  return thrift;
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

// load table from a given table definition section
// td = table definition
std::shared_ptr<TableSpec> loadTable(std::string name, const YAML::Node& td) {
  auto ds = DataSourceUtils::from(td["data"].as<std::string>());
  if (ds == DataSource::NEBULA) {
    return nullptr;
  }

  // hour table level retention.max-hr as single way to ingest and evict data
  const auto retention = td["retention"];

  // kafka requires topic to be set in "kafka section"
  auto kafkaSerde = asKafka(td["kafka"]);
  if (ds == DataSource::KAFKA && kafkaSerde.topic.size() == 0) {
    LOG(WARNING) << "Kafka data requrires topic to be set";
    return nullptr;
  }

  // kafka requires topic to be set in "kafka section"
  auto rocksetSerde = asRockset(td["rockset"]);
  if (ds == DataSource::ROCKSET && rocksetSerde.apiKey.size() == 0) {
    LOG(WARNING) << "Rockset data requrires api key to be set";
    return nullptr;
  }

  // (historical) convention removed that table name follows k.{topic}" for kafka
  // in fact, we allow multiple tables connecting to the same streaming topic
  // max-hr could be fractional value to help us get granularity to seconds
  try {
    return std::make_shared<TableSpec>(
      name,
      retention["max-mb"].as<size_t>(),
      retention["max-hr"].as<double>() * Evidence::HOUR_SECONDS,
      td["schema"].as<std::string>(),
      ds,
      td["loader"].as<std::string>(),
      td["source"].as<std::string>(),
      td["backup"].as<std::string>(),
      DataFormatUtils::from(td["format"].as<std::string>()),
      asCsvProps(td["csv"]),
      asJsonProps(td["json"]),
      asThriftProps(td["thrift"]),
      kafkaSerde,
      rocksetSerde,
      asColumnProps(td["columns"]),
      asTimeSpec(td["time"]),
      asAccessRules(td["access"]),
      asBucketInfo(td["bucket"]),
      asSettings(td["settings"]),
      asMacroValues(td["macros"]),
      asHeaders(td["headers"]));
  } catch (std::exception& ex) {
    LOG(ERROR) << "Error creating table spec: " << name << " - " << ex.what();
    return nullptr;
  }
}

inline void processTableDefinitions(
  const unordered_map<std::string, YAML::Node>& defs,
  unordered_set<std::string>& names,
  TableSpecSet& tables) noexcept {
  // loading all dynamic table definitions from service calls
  for (auto it = defs.begin(); it != defs.end(); ++it) {
    const auto& name = it->first;
    if (names.find(name) != names.end()) {
      LOG(WARNING) << "Skip the same table name already defined: " << name;
      continue;
    }

    // put the name in
    names.emplace(name);

    // max-hr could be fractional value to help us get granularity to seconds
    auto tsp = loadTable(name, it->second);
    if (tsp) {
      tables.emplace(tsp);
    }
  }
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

  // avoid duplicate table name definition
  TableSpecSet tableSet;
  unordered_set<std::string> nameSet;
  unordered_map<std::string, YAML::Node> configTables;
  for (YAML::const_iterator it = tables.begin(); it != tables.end(); ++it) {
    configTables.emplace(it->first.as<std::string>(), std::move(it->second));
  }

  // load tables from config first and then runtime tables
  processTableDefinitions(configTables, nameSet, tableSet);
  processTableDefinitions(runtimeTables_, nameSet, tableSet);

  // swap with new table set
  std::swap(tables_, tableSet);
  stateChanged_ = false;

  // if user mistakenly config things at top level
  // (I made mistake to place a new table the same level as "tables")
  // let's fail this case as mis-config [version, server, nodes, tables]
  if (config.size() > topLevels) {
    throw NException("Un-recoganized config at the top level");
  }
}

// add table definition - note that this will overwrite existing entry keyed by table
// so make sure table name is unique
std::string ClusterInfo::addTable(const std::string& table, const std::string& yaml) {
  auto itr = runtimeTables_.find(table);
  if (itr != runtimeTables_.end()) {
    LOG(WARNING) << "Overwriting existing table: " << table;
  }

  try {
    YAML::Node tableDef = YAML::Load(yaml);
    if (tableDef.size() == 0) {
      return "Invalid yaml for table definition";
    }

    // overwrite - emplace will not overwrite if key exists
    runtimeTables_[table] = std::move(tableDef);
    stateChanged_ = true;
  } catch (std::exception& ex) {
    // failed to parse the yaml text
    LOG(ERROR) << "Failed to parse yaml: " << yaml;
  }
  return {};
}

} // namespace meta
} // namespace nebula