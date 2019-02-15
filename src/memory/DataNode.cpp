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

#include "DataNode.h"

DEFINE_int32(META_PAGE_SIZE, 4 * 1024, "page size for meta data");
DEFINE_int32(DATA_PAGE_SIZE, 256 * 1024, "page size for real data");

/**
 * A data node holds real memory data for each node in the schema tree
 * 
 */
namespace nebula {
namespace memory {

using nebula::type::Kind;
using nebula::type::Schema;
using nebula::type::TreeNode;
using nebula::type::TypeBase;
using nebula::type::TypeNode;

// static method to build node tree
DataNode::DataTree DataNode::buildDataTree(const Schema& schema) {
  // traverse the whole schema tree to generate a data tree
  auto dataTree = schema->treeWalk<TreeNode>(
    [](const auto& v) {},
    [](const auto& v, std::vector<TreeNode>& children) {
      return TreeNode(new DataNode(dynamic_cast<const TypeBase&>(v), children));
    });

  return std::static_pointer_cast<DataNode>(dataTree);
}

} // namespace memory
} // namespace nebula