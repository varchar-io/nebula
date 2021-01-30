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

#include <string>
#include <vector>

/**
 * Define access control data model.
 * 
 * An access control data model can be owned by any level of objects
 * such as table object or column object for table or column level access control respectively.
 * 
 * The data model can be queried through a generic meta data tier 
 * from where pluggable provider can provide content of these data models.
 * 
 * By default, we allow cluster configuraton to define these models for each table/column.
 * Hence, it is a default provider in Nebula.
 */
namespace nebula {
namespace meta {
// TODO(cao): define where a rule is applied.
// READ (including UDF), aggregation (UDAF), WRITE
enum class AccessType {
  UNKNOWN,
  READ,
  AGGREGATION,
  WRITE
};

// TODO(cao): action type is generic but here it means more about "reject"
// it defines when rules are not met, how to reject request, mask data or fail the query.
enum ActionType {
  PASS = 0,
  MASK = 1,
  DENY = 2
};

struct AccessRule {
  explicit AccessRule(AccessType accessType, std::vector<std::string> gps, ActionType actionType)
    : type{ accessType }, groups{ std::move(gps) }, action{ actionType } {}

  AccessType type;
  // TODO(cao): this may not be generic enough
  // it initially means security group (LDAP or else a user belongs to).
  std::vector<std::string> groups;
  ActionType action;
};

// define an alias of access spec
using AccessSpec = std::vector<AccessRule>;

} // namespace meta
} // namespace nebula