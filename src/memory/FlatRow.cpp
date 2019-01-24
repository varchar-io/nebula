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

#include "FlatRow.h"

namespace nebula {
namespace memory {

using nebula::type::TypeBase;

// can be called multiple times to reuse the same flat row
void FlatRow::init() {
  cursor_ = 0;

  // clear stack
  while (!stack_.empty()) {
    stack_.pop();
  }
}

void FlatRow::begin(const TypeNode& node, size_t items) {
  // push the the node KIND
  LOG(INFO) << "begin writing NODE: " << node->name() << " for items: " << items;
  stack_.push(std::make_unique<DataMeta>(node));
}

// return total size
size_t FlatRow::end(const TypeNode& node) {
  // ensure poping the same node as pushed
  N_ENSURE(node.get() == (stack_.top()->node.get()), "same node");
  stack_.pop();

  LOG(INFO) << " end writing NDOE: " << node->name();
  return cursor_;
}

// locate a node by given name (only top level?)
TypeNode FlatRow::locate(const std::string& name) {
  for (size_t i = 0, size = schema_->size(); i < size; ++i) {
    auto node = schema_->childType(i);
    if (name == node->name()) {
      return node;
    }
  }

  return {};
}

} // namespace memory
} // namespace nebula