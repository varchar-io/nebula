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

#include "execution/BlockManager.h"
#include "execution/io/trends/Comments.h"
#include "execution/io/trends/Pins.h"
#include "execution/io/trends/Trends.h"
#include "meta/MetaService.h"
#include "meta/NNode.h"
#include "meta/Table.h"
#include "meta/TestTable.h"

/**
 * 
 * Define table meta data service provider for nebula execution runtime.
 * 
 */
namespace nebula {
namespace execution {
namespace meta {

class TableService : public nebula::meta::MetaService {
public:
  TableService() {
#define ADD_TYPE(T)                  \
  {                                  \
    auto t1 = std::make_shared<T>(); \
    presets_[t1->name()] = t1;       \
  }

    ADD_TYPE(nebula::meta::TestTable)
    ADD_TYPE(nebula::execution::io::trends::SignaturesTable)
    ADD_TYPE(nebula::execution::io::trends::CommentsTable)
    ADD_TYPE(nebula::execution::io::trends::PinsTable)
    ADD_TYPE(nebula::execution::io::trends::TrendsTable)

#undef ADD_TYPE
  }
  virtual ~TableService() = default;

  virtual std::shared_ptr<nebula::meta::Table> query(const std::string& name) override {
    auto it = presets_.find(name);
    if (it != presets_.end()) {
      return it->second;
    }

    return std::make_shared<nebula::meta::Table>(name);
  }

  virtual std::vector<nebula::meta::NNode> queryNodes(
    const std::shared_ptr<nebula::meta::Table>,
    std::function<bool(const nebula::meta::NNode&)>) override;

private:
  // preset is a list of preload table before meta service functions
  std::unordered_map<std::string, std::shared_ptr<nebula::meta::Table>> presets_;
};
} // namespace meta
} // namespace execution
} // namespace nebula