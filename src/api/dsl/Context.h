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

#include <glog/logging.h>

#include "Expressions.h"

#include "api/udf/Not.h"
#include "common/Cursor.h"
#include "execution/ExecutionPlan.h"
#include "meta/ClusterInfo.h"
#include "meta/MetaService.h"
#include "meta/Table.h"
#include "surface/DataSurface.h"

/**
 * Define query context: such as who is sending the query, for what purpose.
 * This context will be used for security check and context logging for usgae analysis.
 * 
 * Most likely this will be an open structure with all information optional.
 */
namespace nebula {
namespace api {
namespace dsl {

// store error in context
enum class Error {
  NONE,
  NO_SELECTS,
  NO_AUTH,
  TABLE_PERM,
  COLUMN_PERM,
  NOT_SUPPORT,
  INVALID_QUERY
};

class QueryContext {
public:
  QueryContext(const std::string& user, std::unordered_set<std::string> groups)
    : user_{ user }, groups_{ std::move(groups) }, error_{ Error::NONE } {}

  inline bool isAuth() const {
    // any authorized uesr will have at least one group regardless what the name is
    return !groups_.empty();
  }

  inline const std::string& user() const {
    return user_;
  }

  inline const std::unordered_set<std::string>& groups() const {
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

private:
  // user context to understand who is making the query
  std::string user_;
  std::unordered_set<std::string> groups_;
  Error error_;
};
} // namespace dsl
} // namespace api
} // namespace nebula