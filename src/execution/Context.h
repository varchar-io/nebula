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

#include "common/Hash.h"
#include "meta/ClusterInfo.h"

/**
 * Define query context: such as who is sending the query, for what purpose.
 * This context will be used for security check and context logging for usgae analysis.
 * 
 * Most likely this will be an open structure with all information optional.
 */
namespace nebula {
namespace execution {

// store error in context
enum class Error {
  NONE,
  NO_SELECTS,
  NO_AUTH,
  TABLE_PERM,
  COLUMN_PERM,
  NOT_SUPPORT,
  INVALID_QUERY,
  INVALID_METRIC
};

// recording query stats
struct QueryStats {
  explicit QueryStats()
    : blocksScan{ 0 },
      rowsScan{ 0 },
      rowsRet{ 0 } {}
  // blocks scanned in given compute
  size_t blocksScan;
  // rows scanned in given compute
  size_t rowsScan;
  // rows returned in given compute
  size_t rowsRet;
  inline std::string toString() const {
    return fmt::format("blocks scan:{0}, rows scan: {1}, rows returned: {2}",
                       blocksScan, rowsScan, rowsRet);
  }
};

class QueryContext {
public:
  QueryContext(const std::string& user, nebula::common::unordered_set<std::string> groups)
    : user_{ user },
      groups_{ std::move(groups) },
      error_{ Error::NONE },
      stats_{} {}

  inline bool isAuth() const {
    // any authorized uesr will have at least one group regardless what the name is
    return !groups_.empty();
  }

  inline const std::string& user() const {
    return user_;
  }

  inline const nebula::common::unordered_set<std::string>& groups() const {
    return groups_;
  }

  inline void setError(Error error) {
    error_ = error;
  }

  inline Error getError() const {
    return error_;
  }

  inline bool requireAuth() const {
    // check if current system requires auth
    return nebula::meta::ClusterInfo::singleton().server().authRequired;
  }

  inline QueryStats& stats() {
    return stats_;
  }

  inline static std::unique_ptr<QueryContext> def() {
    return std::make_unique<QueryContext>(
      "nebula", nebula::common::unordered_set<std::string>{ "nebula-users" });
  }

private:
  // user context to understand who is making the query
  std::string user_;
  nebula::common::unordered_set<std::string> groups_;
  Error error_;
  QueryStats stats_;
};

} // namespace execution
} // namespace nebula