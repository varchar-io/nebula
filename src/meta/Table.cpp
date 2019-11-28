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

#include "Table.h"

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace meta {

void Table::loadTable() {
  // TODO(cao) - really build up meta service to servce this function
  // throw NException("Meta service not implemented yet.");
}

ActionType access(AccessType at, const std::unordered_set<std::string>& groups, const std::vector<AccessRule>& rules) {
  // TODO(cao): assuming only one rule defined for each access type.
  // Need to support combination logic of multi-rules in the future.
  auto rule = std::find_if(rules.begin(), rules.end(), [at](const AccessRule& r) {
    return r.type == at;
  });

  // no rules defined for given access type, grant pass by default
  if (rule == rules.end()) {
    return ActionType::PASS;
  }

  // check required security groups could be found in given groups
  // TODO(cao): need to support logical ops of defined groups, requiring all or any.
  // By default, any group found in given groups will grant pass.
  const auto& required = rule->groups;
  if (std::any_of(required.begin(), required.end(), [&groups](const auto& g) {
        return groups.find(g) != groups.end();
      })) {
    return ActionType::PASS;
  }

  // return defined rejection, it won't be PASS
  return rule->action;
}

ActionType Table::checkAccess(AccessType at, const std::unordered_set<std::string>& groups, const std::string& col) const {
  // get access rules to check on
  // table level
  if (col.empty()) {
    return access(at, groups, this->rules_);
  }

  return access(at, groups, column(col).rules);
}
} // namespace meta
} // namespace nebula