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

#include <fmt/format.h>
#include <folly/Conv.h>
#include <pg_query.h>
#include <rapidjson/document.h>

#include "SpecRepo.h"
#include "common/Evidence.h"
#include "storage/NFS.h"
#include "storage/kafka/KafkaTopic.h"

DEFINE_uint64(KAFKA_SPEC_ROWS,
              10000,
              "rows per sepc for kafka ingestion"
              "this value is used in spec identifier so do not modify");
DEFINE_uint64(KAFKA_TIMEOUT_MS, 5000, "Timeout of each Kafka API call");

/**
 * We will sync etcd configs for cluster info into this memory object
 * To understand cluster status - total nodes.
 */
namespace nebula {
namespace ingest {

using dsu = nebula::meta::DataSourceUtils;
using nebula::common::Evidence;
using nebula::common::unordered_map;
using nebula::meta::ClusterInfo;
using nebula::meta::DataSource;
using nebula::meta::DataSourceUtils;
using nebula::meta::NNode;
using nebula::meta::NNodeSet;
using nebula::meta::TableSpecPtr;
using nebula::meta::TimeSpec;
using nebula::meta::TimeType;
using nebula::storage::FileInfo;
using nebula::storage::kafka::KafkaSegment;
using nebula::storage::kafka::KafkaTopic;

// specified batch size in table config - not kafka specific
constexpr auto S_BATCH = "batch";
// specified kafka partition /offset to consume - kafka specific
constexpr auto S_PARTITION = "k.partition";
constexpr auto S_OFFSET = "k.offset";

// generate a list of ingestion spec based on cluster info
void SpecRepo::refresh(const ClusterInfo& ci) noexcept {
  // we only support adding new spec to the repo
  // if a spec is already in repo, we skip it
  // for some use case such as data refresh, it will have the same signature
  // if data is newer (e.g file size + timestamp), we should mark it as replacement.
  std::vector<std::shared_ptr<IngestSpec>> specs;
  const auto& tableSpecs = ci.tables();

  // generate a version all spec to be made during this batch: {config version}_{current unix timestamp}
  const auto version = fmt::format("{0}.{1}", ci.version(), Evidence::unix_timestamp());
  for (auto itr = tableSpecs.cbegin(); itr != tableSpecs.cend(); ++itr) {
    process(version, *itr, specs);
  }

  // interpret all specs to mark their status
  update(specs);
}

std::string buildIdentityByTime(const TimeSpec& time) {
  switch (time.type) {
  case TimeType::STATIC: {
    // the static time stamp value is its identity
    return folly::to<std::string>(time.unixTimeValue);
  }
  case TimeType::CURRENT: {
    return folly::to<std::string>(Evidence::unix_timestamp());
  }
  default: return "";
  }
}

// this method is to generate one spec per file
void genSpecPerFile(const TableSpecPtr& table,
                    const std::string& version,
                    const std::vector<FileInfo>& files,
                    std::vector<std::shared_ptr<IngestSpec>>& specs,
                    size_t watermark) noexcept {
  for (auto itr = files.cbegin(), end = files.cend(); itr != end; ++itr) {
    if (!itr->isDir) {
      // we do not generate empty files
      if (itr->size == 0) {
        VLOG(1) << "Skip an empty file to scan: " << itr->name;
        continue;
      }

      // generate a ingest spec from given file info
      // use name as its identifier
      auto spec = std::make_shared<IngestSpec>(
        table, version, itr->name, itr->domain, itr->size, SpecState::NEW, watermark);

      // push to the repo
      specs.push_back(spec);
    }
  }
}

// generate specs for swap type of data
// for swap type, we need file system support:
//  1. list files with timestamp
//  2. each file name will be used as identifier and timestamp will distinguish different data
void genSpecs4Swap(const std::string& version,
                   const TableSpecPtr& table,
                   std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  if (dsu::isFileSystem(table->source)) {
    // parse location to get protocol, domain/bucket, path
    auto sourceInfo = nebula::storage::parse(table->location);

    // making a s3 fs with given host
    auto fs = nebula::storage::makeFS(dsu::getProtocol(table->source), sourceInfo.host);

    // list all objects/files from given path
    auto files = fs->list(sourceInfo.path);
    genSpecPerFile(table, version, files, specs, 0);
    return;
  }

  LOG(WARNING) << "only s3 supported for now";
}

// iterative replace pathTemplate with current level of pattern macro
void SpecRepo::genPatternSpec(long start,
                              nebula::meta::PatternMacro curr,
                              nebula::meta::PatternMacro dest,
                              std::time_t now,
                              const std::string& pathTemplate,
                              std::time_t cutOffTime,
                              const std::string& version,
                              const TableSpecPtr& table,
                              std::vector<std::shared_ptr<IngestSpec>>& specs) {

  const auto curUnitInSeconds = nebula::meta::unitInSeconds.at(curr);
  const auto curMacroStr = nebula::meta::patternMacroStr.at(curr);
  const auto childMarco = nebula::meta::childPattern.at(curr);
  const auto startChildPatternIndex = nebula::meta::childSize.at(childMarco) - 1;
  const auto sourceInfo = nebula::storage::parse(table->location);

  auto fs = nebula::storage::makeFS(dsu::getProtocol(table->source), sourceInfo.host);

  for (long i = start; i >= 0; i--) {
    auto str = pathTemplate;
    const auto watermark = now - i * curUnitInSeconds;

    const auto macroWithBracket = fmt::format("{{{0}}}", curMacroStr);
    const auto opos = pathTemplate.find(macroWithBracket);

    // uppercase pattern string
    std::string upperCurPatternStr;
    transform(macroWithBracket.begin(), macroWithBracket.end(), std::back_inserter(upperCurPatternStr), toupper);
    const auto upos = pathTemplate.find(macroWithBracket);

    // check original case and upper cased macro in pathTemplate
    const auto pos = opos != std::string::npos ? opos : upos;

    // check declared macro used
    N_ENSURE(pos != std::string::npos, "pattern not found");

    std::string timeFormat;
    switch (curr) {
    case nebula::meta::PatternMacro::DAILY:
      timeFormat = Evidence::fmt_ymd_dash(watermark);
      break;
    case nebula::meta::PatternMacro::HOURLY:
      timeFormat = Evidence::fmt_hour(watermark);
      break;
    case nebula::meta::PatternMacro::MINUTELY:
      timeFormat = Evidence::fmt_minute(watermark);
      break;
    case nebula::meta::PatternMacro::SECONDLY:
      timeFormat = Evidence::fmt_second(watermark);
      break;
    default:
      LOG(ERROR) << "timestamp or invalid format not handled";
    }

    const auto path = str.replace(pos, macroWithBracket.size(), timeFormat);

    // watermark is mono incremental when curr == dest, always smaller or equal when scan child marco
    if (watermark < cutOffTime) continue;

    if (curr == dest) {
      genSpecPerFile(table, version, fs->list(path), specs, watermark);
    } else {
      genPatternSpec(startChildPatternIndex, childMarco, dest, watermark, path, cutOffTime, version, table, specs);
    }
  }
}

void SpecRepo::genSpecs4Roll(const std::string& version,
                             const TableSpecPtr& table,
                             std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  if (dsu::isFileSystem(table->source)) {
    // parse location to get protocol, domain/bucket, path
    auto sourceInfo = nebula::storage::parse(table->location);

    // making a s3 fs with given host
    auto fs = nebula::storage::makeFS(dsu::getProtocol(table->source), sourceInfo.host);

    auto pt = nebula::meta::extractPatternMacro(table->timeSpec.pattern);

    // list all objects/files from given path
    // A roll spec will cover X days given table location of source data
    const auto now = Evidence::now();
    const auto maxDays = table->max_seconds / Evidence::DAY_SECONDS;

    // earliest time in second to interpret in ascending order
    long cutOffTime = now - table->max_seconds;

    // TODO(chenqin): don't support other macro other than dt=date/hr=hour/mi=minute/se=second yet.
    genPatternSpec(maxDays,
                   nebula::meta::PatternMacro::DAILY,
                   pt,
                   now,
                   sourceInfo.path,
                   cutOffTime,
                   version,
                   table,
                   specs);
    return;
  }

  LOG(WARNING) << "file system not supported for now: "
               << DataSourceUtils::getProtocol(table->source);
}

void genKafkaSpec(const std::string& version,
                  const TableSpecPtr& table,
                  std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  // visit each partition of the topic and figure out range for each spec
  // stream is different as static file, to make it reproducible, we need
  // a stable spec generation based on offsets, every N (eg. 10K) records per spec
  KafkaTopic topic(table->location, table->name, table->serde, FLAGS_KAFKA_TIMEOUT_MS);

  // check if this table has set batch size to overwrite the default one
  const auto& settings = table->settings;
  auto batch = FLAGS_KAFKA_SPEC_ROWS;
  auto itr = settings.find(S_BATCH);
  if (itr != settings.end()) {
    batch = folly::to<size_t>(itr->second);
    LOG(INFO) << "Table " << table->name << " overwrite batch size as " << batch;
  }

  // turn these segments into ingestion spec
  auto convert = [&specs, &table, &version](const std::list<KafkaSegment>& segments) {
    // turn these segments into ingestion spec
    for (auto itr = segments.cbegin(), end = segments.cend(); itr != end; ++itr) {
      specs.push_back(std::make_shared<IngestSpec>(
        table, version, itr->id(), "kafka", itr->size, SpecState::NEW, 0));
    }
  };

  // if specific partition / offset specified, we only consume it.
  // this is usually for debugging purpose
  auto itr_p = settings.find(S_PARTITION);
  auto itr_o = settings.find(S_OFFSET);
  if (itr_p != settings.end() && itr_o != settings.end()) {
    std::list<KafkaSegment> segments;
    segments.emplace_back(folly::to<int32_t>(itr_p->second), folly::to<int64_t>(itr_o->second), batch);
    convert(segments);
    return;
  }

  // set start time
  const auto startMs = 1000 * (Evidence::unix_timestamp() - table->max_seconds);
  auto segments = topic.segmentsByTimestamp(startMs, batch);
  convert(segments);
}
bool replace(std::string& str, const std::string& from, const std::string& to) {
  size_t start_pos = str.find(from);
  if (start_pos == std::string::npos)
    return false;
  str.replace(start_pos, from.length(), to);
  return true;
}

void SpecRepo::process(
  const std::string& version,
  const TableSpecPtr& table,
  std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  // specialized loader handling - nebula test set identified by static time provided
  if (table->loader == "NebulaTest") {
    // single spec for nebula test loader
    specs.push_back(std::make_shared<IngestSpec>(
      table, version, buildIdentityByTime(table->timeSpec), table->name, 0, SpecState::NEW, 0));
    return;
  }
  //hack, if table has sql statement, switch to sql parser
  if (!table->ddl.empty()) {
    auto postgresSQL = pg_query_parse(table->ddl.c_str());
    rapidjson::Document doc;
    if (doc.Parse(postgresSQL.parse_tree).HasParseError() || doc.GetArray().Empty()) {
      LOG(ERROR) << postgresSQL.parse_tree << "ddl has syntax error use this https://rextester.com/l/postgresql_online_compiler";
      return;
    }
    LOG(INFO) << postgresSQL.parse_tree;
    pg_query_free_parse_result(postgresSQL);
    interpret(version, table, specs, doc);
  }

  // S3 has two mode:
  // 1. swap data when renewed or
  // 2. roll data clustered by time
  if (dsu::isFileSystem(table->source)) {
    if (table->loader == "Swap") {
      genSpecs4Swap(version, table, specs);
      return;
    }

    if (table->loader == "Roll") {
      genSpecs4Roll(version, table, specs);
      return;
    }

    if (table->loader == "Api") {
      // We're not loading data for API
      return;
    }
  }

  if (table->source == DataSource::KAFKA) {
    genKafkaSpec(version, table, specs);
    return;
  }

  LOG(WARNING) << fmt::format("Unsupported loader: {0} for table {1}",
                              table->loader, table->toString());
}

// supported property name after where clause
inline void assign(const TableSpecPtr& table, const std::string& config_name, const std::string config_val, const int iconfig_val, const float fconfig_val) {
  const bool isFormat = config_name.find("format") != std::string::npos;
  const bool isLoader = config_name.find("loader") != std::string::npos;
  const bool isMaxMB = config_name.find("max_mb") != std::string::npos;
  const bool isMaxHR = config_name.find("max_hr") != std::string::npos;
  const bool isTopic = config_name.find("topic") != std::string::npos;
  const bool isSource = config_name.find("source") != std::string::npos;
  // we might consider support create table to better support schema typing system
  // but it seems overkill at this time point.
  const bool isSchema = config_name.find("schema") != std::string::npos;
  const bool isTimestamp = config_name.find("timestamp") != std::string::npos;
  const bool isBatch = config_name.find("batch") != std::string::npos;
  const bool isTopicRetention = config_name.find("topic_retention") != std::string::npos;
  const bool isProtocol = config_name.find("protocol") != std::string::npos;
  const bool isTimePartition = config_name.find("time_partition") != std::string::npos;

  if (isTopic) {
    table->name = config_val;
  }

  if (isSource && table->location.empty()) {
    table->location = config_val;
  }
  if (isFormat) {
    table->format = config_val;
  }
  if (isLoader) {
    table->loader = config_val;
  }
  if (isMaxMB) {
    table->max_mb = iconfig_val;
  }
  if (isMaxHR) {
    table->max_seconds = fconfig_val * 3600;
  }
  if (isSchema) {
    table->schema = config_val;
  }
  if (isTimestamp) {
    //hardcode
    table->timeSpec.type = TimeType::MACRO;
  }
  if (isBatch) {
    auto setting = table->settings;
    std::ostringstream ss;
    ss << iconfig_val;
    std::string s(ss.str());
    setting.insert({ "batch", s });
  }
  if (isProtocol) {
    table->serde.protocol = config_val;
  }

  if (isTopicRetention) {
    table->serde.retention = iconfig_val;
  }

  if (isTimePartition) {
    table->timeSpec.pattern = config_val;
  }
}

void SpecRepo::interpret(const std::string& version,
                         const TableSpecPtr& table,
                         std::vector<std::shared_ptr<IngestSpec>>& specs,
                         const rapidjson::Document& doc) {
  // we only need one pass to get udfs
  table->udfs.clear();

  // track permutation of locations by replace macro(s) in table location
  std::vector<std::string> locations;
  std::vector<std::string> paths;
  locations.push_back(table->location);

  for (auto& statements : doc.GetArray()) {
    auto root = statements.GetObject();
    auto head = root.FindMember("RawStmt")->value.GetObject().FindMember("stmt")->value.GetObject();
    bool isView = head.FindMember("ViewStmt") != head.MemberEnd();
    bool isSelect = head.FindMember("SelectStmt") != head.MemberEnd();
    assert(isSelect || isView);

    // extract view namespace and name
    if (isView) {
      auto RangeVar = head.FindMember("ViewStmt")->value.GetObject().FindMember("view")->value.GetObject().FindMember("RangeVar")->value.GetObject();
      auto ns = RangeVar.FindMember("schemaname")->value.GetString();
      auto db = RangeVar.FindMember("relname")->value.GetString();
      // LOG(INFO) << "view name " << ns << "." << db;
      // assign name
      table->name = std::string(ns).append(db);
    }

    // extract columns and UDFs
    // for view statement, we need to ViewStmt/query/SelectStmt
    auto selectStmt = isView ? head.FindMember("ViewStmt")->value.GetObject().FindMember("query")->value.GetObject().FindMember("SelectStmt")->value.GetObject() : head.FindMember("SelectStmt")->value.GetObject();
    auto columns = selectStmt.FindMember("targetList")->value.GetArray();

    for (auto& c : columns) {
      auto in = c.GetObject();
      bool hasAlias = in.FindMember("ResTarget")->value.GetObject().FindMember("name") != in.FindMember("ResTarget")->value.GetObject().MemberEnd();
      auto val = in.FindMember("ResTarget")->value.GetObject().FindMember("val")->value.GetObject();
      bool isFuncCall = val.FindMember("FuncCall") != val.MemberEnd();
      bool isTypeCast = val.FindMember("TypeCast") != val.MemberEnd();
      bool isColumn = val.FindMember("ColumnRef") != val.MemberEnd();
      assert(isFuncCall || isColumn || isTypeCast);
      std::string alias = hasAlias ? in.FindMember("ResTarget")->value.GetObject().FindMember("name")->value.GetString() : "";

      if (isColumn) {
        auto fields = val.FindMember("ColumnRef")->value.FindMember("fields")->value.GetArray();
        for (auto& f : fields) {
          auto fieldName = f.GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();
          std::pair<std::string, std::string> col("", fieldName);
          table->udfs.push_back(col);
        }
      } else if (isFuncCall) {
        auto funcs = val.FindMember("FuncCall")->value.FindMember("funcname")->value.GetArray();
        auto args = val.FindMember("FuncCall")->value.FindMember("args")->value.GetArray();
        rapidjson::Value ::ConstValueIterator funcitr = funcs.Begin();
        rapidjson::Value ::ConstValueIterator argitr = args.Begin();
        while (funcitr != funcs.End()) {
          auto funcName = funcitr->GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();

          std::string argument_list = "";
          // function without arguments
          if (argitr != args.End()) {
            bool isMulitValue = argitr->GetObject().FindMember("A_Expr") != argitr->GetObject().MemberEnd();
            // TODO(chenqin): only support MYUDF(field) should support UDF involving multi fields e.g MYUDF(id + flag),
            assert(!isMulitValue);
            auto fields = argitr->GetObject().FindMember("ColumnRef")->value.FindMember("fields")->value.GetArray();

            for (auto& f : fields) {
              auto fieldName = f.GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();
              argument_list.append(fieldName);
            }
            argitr++;
          }
          std::pair<std::string, std::string> col(funcName, argument_list);
          table->udfs.push_back(col);
          funcitr++;
        }
      } else if (isTypeCast) {
        auto args = val.FindMember("TypeCast")->value.FindMember("arg")->value.FindMember("ColumnRef")->value.FindMember("fields")->value.GetArray();
        auto typeNameField = val.FindMember("TypeCast")->value.FindMember("typeName")->value.FindMember("TypeName")->value.FindMember("names")->value.GetArray();

        std::string typeName; //full name of type cast to e.g pg_catalog.int8
        for (const auto& na : typeNameField) {
          auto name = na.GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();
          typeName.append(typeName.empty() ? name : "." + std::string(name));
        }

        for (auto& f : args) {
          auto fieldName = f.GetObject().FindMember("String")->value.GetObject().FindMember("str")->value.GetString();
          std::pair<std::string, std::string> col("cast " + typeName + " as " + alias, fieldName);
          table->udfs.push_back(col);
        }
      }
    }

    auto fromTables = selectStmt.FindMember("fromClause")->value.GetArray();
    for (auto& t : fromTables) {
      auto name = t.GetObject().FindMember("RangeVar")->value.GetObject().FindMember("relname")->value.GetString();
      table->source = DataSourceUtils::getSourceByName(name);
    }

    auto whereClauses = selectStmt.FindMember("whereClause")->value.GetObject();
    bool boolExpr = whereClauses.FindMember("BoolExpr") != whereClauses.MemberEnd();
    // only support boolexp for now
    assert(boolExpr);

    auto arguments = whereClauses.FindMember("BoolExpr")->value.GetObject().FindMember("args")->value.GetArray();
    for (auto& arg : arguments) {
      std::string config_name, config_ref;
      std::string config_val; // String config_val
      int iconfig_val;        // Integer config_val
      float fconfig_val;      // Float config_val

      bool isAExpr = arg.GetObject().FindMember("A_Expr") != arg.GetObject().MemberEnd();
      assert(isAExpr);

      auto aExpr = arg.GetObject().FindMember("A_Expr")->value.GetObject();
      auto names = aExpr.FindMember("name")->value.GetArray();

      for (auto& nm : names) {
        auto exp = nm.FindMember("String")->value.FindMember("str")->value.GetString();
        config_ref.append(config_ref.empty() ? exp : "." + std::string(exp));
      }

      auto lexpr = aExpr.FindMember("lexpr")->value.GetObject();
      auto columnRef = lexpr.FindMember("ColumnRef")->value.GetObject();
      auto fields = columnRef.FindMember("fields")->value.GetArray();
      for (auto& f : fields) {
        auto exp = f.FindMember("String")->value.FindMember("str")->value.GetString();
        config_name.append(config_name.empty() ? exp : "." + std::string(exp));
      }

      const bool isMacro = config_name.find("macro.") != std::string::npos;

      bool arrRExpr = aExpr.FindMember("rexpr")->value.IsArray();
      if (arrRExpr) {
        auto rexprs = aExpr.FindMember("rexpr")->value.GetArray();
        // handle collection xx in (aa, bb)
        for (auto& expr : rexprs) {
          bool aConst = expr.FindMember("A_Const") != expr.MemberEnd();
          if (aConst) {
            auto val = expr.FindMember("A_Const")->value.GetObject().FindMember("val")->value.GetObject();
            bool isFloat = val.FindMember("Float") != val.MemberEnd();
            bool isString = val.FindMember("String") != val.MemberEnd();
            bool isInteger = val.FindMember("Integer") != val.MemberEnd();
            assert(isFloat || isString || isInteger);
            if (isInteger) {
              auto ival = val.FindMember("Integer")->value.FindMember("ival")->value.GetInt();
              iconfig_val = ival;
            } else {
              auto constval = val.FindMember(isFloat ? "Float" : "String")->value.FindMember("str")->value.GetString();
              config_val = constval;
              if (isFloat) {
                fconfig_val = std::stof(constval);
              }
            }
            // recursive replace location macro and call interpret
            if (isMacro) {
              // do str matching requires convert to string
              const auto macro = "%7B" + config_name.substr(6) + "%7D";
              for (auto s : locations) {
                if (s.find(macro) != std::string::npos) {
                  std::string path(s);
                  std::string macroValue = isInteger ? std::to_string(iconfig_val) : config_val;
                  replace(path, macro, macroValue);
                  LOG(INFO) << "replacing macro " << macro << " with: " << path;
                  paths.push_back(path);
                }
              }
            }
          }
        }
      } else {
        auto rexpr = aExpr.FindMember("rexpr")->value.GetObject();
        // hack, copy code from above
        bool aConst = rexpr.FindMember("A_Const") != rexpr.MemberEnd();
        if (aConst) {
          auto val = rexpr.FindMember("A_Const")->value.GetObject().FindMember("val")->value.GetObject();
          bool isFloat = val.FindMember("Float") != val.MemberEnd();
          bool isString = val.FindMember("String") != val.MemberEnd();
          bool isInteger = val.FindMember("Integer") != val.MemberEnd();
          assert(isFloat || isString || isInteger);
          if (isInteger) {
            auto ival = val.FindMember("Integer")->value.FindMember("ival")->value.GetInt();
            // LOG(INFO) << "rexpr : " << ival;
            iconfig_val = ival;
          } else {
            auto constval = val.FindMember(isFloat ? "Float" : "String")->value.FindMember("str")->value.GetString();
            //LOG(INFO) << "rexpr : " << constval;
            config_val = constval;
            if (isFloat) {
              fconfig_val = std::stof(constval);
            }
          }
        }
      }
      ::nebula::ingest::assign(table, config_name, config_val, iconfig_val, fconfig_val);
      if (isMacro) {
        locations = paths;
        paths.clear();
      }
    }
  }

  // if s3 location empty, need extra loop get table->location assigned before interpret
  if (table->location.empty() && table->source == DataSource::S3) {
    process(version, table, specs);
    return;
  }

  // permute macro replaced locations
  for (auto& p : locations) {
    // reset field fallback to normal table parsing
    table->ddl = "";
    table->location = p;
    process(version, table, specs);
  }
}

void SpecRepo::update(const std::vector<std::shared_ptr<IngestSpec>>& specs) noexcept {
  // next version of all specs
  unordered_map<std::string, SpecPtr> next;
  next.reserve(specs.size());

  // go through the new spec list and update the existing ones
  // need lock here?
  auto brandnew = 0;
  auto renew = 0;
  auto removed = specs_.size() - specs.size();
  for (auto itr = specs.cbegin(), end = specs.cend(); itr != end; ++itr) {
    // check if we have this spec already?
    auto specPtr = (*itr);
    const auto& sign = specPtr->id();
    auto found = specs_.find(sign);
    if (found == specs_.end()) {
      ++brandnew;
    } else {
      auto prev = found->second;

      // by default, we carry over existing spec's properties
      const auto& node = prev->affinity();
      specPtr->setAffinity(node);
      specPtr->setState(prev->state());

      // TODO(cao) - use only size for the checker for now, may extend to other properties
      // this is an update case, otherwise, spec doesn't change, ignore it.
      if (specPtr->size() != prev->size()) {
        specPtr->setState(SpecState::RENEW);
        ++renew;
      }

      // if the node is not active, we may remove the affinity to allow new assignment
      if (!node.isActive()) {
        specPtr->setAffinity(NNode::invalid());
      }
    }

    // move to the next version
    next.emplace(sign, specPtr);
  }

  // print out update stats
  if (brandnew > 0 || renew > 0 || removed > 0) {
    LOG(INFO) << "Updating " << specs.size()
              << " specs: brandnew=" << brandnew
              << ", renew=" << renew
              << ", removed=" << removed
              << ", count=" << next.size();

    // let's swap with existing one
    if (specs.size() != next.size()) {
      LOG(WARNING) << "No duplicate specs allowed.";
    }

    std::swap(specs_, next);
  }
}

bool SpecRepo::confirm(const std::string& spec, const nebula::meta::NNode& node) noexcept {
  auto f = specs_.find(spec);
  // not found
  if (f == specs_.end()) {
    return false;
  }

  // reuse the same node for the same spec
  auto& sp = f->second;
  if (!sp->assigned()) {
    sp->setAffinity(node);
    return true;
  }

  // not in the same node
  auto& assignment = sp->affinity();
  if (!assignment.equals(node)) {
    LOG(INFO) << "Spec [" << spec << "] moves from " << node.server << " to " << assignment.server;
    return false;
  }

  return true;
}

void SpecRepo::assign(const std::vector<NNode>& nodes) noexcept {
  // we're looking for a stable assignmet, given the same set of nodes
  // this order is most likely having stable order
  // std::sort(nodes.begin(), nodes.end(), [](auto& n1, auto& n2) {
  //   return n1.server.compare(n2.server);
  // });
  const auto size = nodes.size();

  if (size == 0) {
    LOG(WARNING) << "No nodes to assign nebula specs.";
    return;
  }

  size_t idx = 0;

  // for each spec
  // TODO(cao): should we do hash-based shuffling here to ensure a stable assignment?
  // Round-robin is easy to break the position affinity whenever new spec is coming
  // Or we can keep order of the specs so that any old spec is associated.
  for (auto& spec : specs_) {
    // not assigned yet
    auto sp = spec.second;
    if (!sp->assigned()) {
      auto startId = idx;
      while (true) {
        auto& n = nodes.at(idx);
        if (n.isActive()) {
          sp->setAffinity(n);
          idx = (idx + 1) % size;
          break;
        }

        idx = (idx + 1) % size;
        if (idx == startId) {
          LOG(ERROR) << "No active node found to assign spec.";
          return;
        }
      }
    }
  }
}

} // namespace ingest
} // namespace nebula