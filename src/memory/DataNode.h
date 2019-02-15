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

#include "DataSurface.h"
#include "Memory.h"
#include "Tree.h"
#include "Type.h"
#include "glog/logging.h"
#include "serde/TypeData.h"
#include "serde/TypeDataFactory.h"
#include "serde/TypeMetadata.h"

/**
 * A data node holds real memory data for each node in the schema tree
 * 
 */
namespace nebula {
namespace memory {

class DataNode : public nebula::type::Tree<DataNode*> {
public:
  using DataTree = std::shared_ptr<DataNode>;
  static DataTree buildDataTree(const nebula::type::Schema&);

public:
  DataNode(const nebula::type::TypeBase& type)
    : type_{ type },
      meta_{ nebula::memory::serde::TypeDataFactory::createMeta(type.k()) },
      data_{ nebula::memory::serde::TypeDataFactory::createData(type.k()) },
      nebula::type::Tree<DataNode*>(this) {
    LOG(INFO) << "created a data node without children for type: " << type.name();
  }

  DataNode(const nebula::type::TypeBase& type, const std::vector<nebula::type::TreeNode>& children)
    : type_{ type },
      meta_{ nebula::memory::serde::TypeDataFactory::createMeta(type.k()) },
      data_{ nebula::memory::serde::TypeDataFactory::createData(type.k()) },
      nebula::type::Tree<DataNode*>(this, children) {
    LOG(INFO) << "created a data node for type: " << type.name() << " with children: " << children.size();
  }

  virtual ~DataNode() = default;

  inline size_t id() const {
    // TODO(cao): generate identifier for current node
    return 0;
  }

private:
  // pointing to a node in the schema tree which is supposed to be shared.
  // should we make a copy here to be safe?
  const nebula::type::TypeBase& type_;

  // metadata
  std::unique_ptr<nebula::memory::serde::TypeMetadata> meta_;

  // real data
  std::unique_ptr<nebula::memory::serde::TypeData> data_;
};
} // namespace memory
} // namespace nebula