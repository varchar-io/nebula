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

#include "meta/MetaService.h"
#include "meta/NNode.h"
#include "meta/Table.h"
#include "type/Serde.h"

/**
 * This module is to build special case for trends which has some hard coded data.
 * Will be deleted after it's done with pilot run.
 */
namespace nebula {
namespace execution {
namespace io {
namespace trends {
class CommentsTable : public nebula::meta::Table {
  static constexpr auto NAME = "pin.comments";

public:
  CommentsTable() : Table(NAME) {
    // TODO(cao) - let's make date as a number
    // id bigint,
    // board_id bigint,
    // user_id bigint,
    // created_at string => map to _time_ in "Y-M-D H:M:S" format,
    // title string,
    // details string,
    // image_signature string,
    // repins bigint
    schema_ = nebula::type::TypeSerializer::from(
      "ROW<_time_:long, user_id:long, comments:string>");
  }

  virtual ~CommentsTable() = default;

  // load comments data from a local file
  // since we failed AWS integration so far
  void load(const std::string&);

  virtual std::shared_ptr<nebula::meta::MetaService> getMs() const override;

  virtual nebula::meta::Column column(const std::string& col) const noexcept override {
    if (col == "user_id") {
      // enable bloom filter on id column
      return { true };
    }

    return Table::column(col);
  }
};

class CommentsMetaService : public nebula::meta::MetaService {
public:
  virtual std::shared_ptr<nebula::meta::Table> query(const std::string&) override {
    return std::make_shared<CommentsTable>();
  }

  virtual std::vector<nebula::meta::NNode> queryNodes(
    const std::shared_ptr<nebula::meta::Table>,
    std::function<bool(const nebula::meta::NNode&)>) override {
    return { nebula::meta::NNode::local() };
  }
};

} // namespace trends
} // namespace io
} // namespace execution
} // namespace nebula