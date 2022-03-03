/*
 * Copyright 2020-present
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
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "meta/NodeManager.h"

namespace nebula {
namespace meta {
namespace test {

// test node manager
TEST(NodeManagerTest, TestNodeManagement) {
  // node manager for config mode - static nodes
  {
    auto nm = NodeManager::create({ { NRole::NODE, "128.0.0.1", 9199 }, { NRole::NODE, "128.0.0.2", 9199 } });
    const auto nodes = nm->nodes();
    EXPECT_EQ(nodes.size(), 2);

    // mark a node to be bad
    nm->mark("128.0.0.1:9199");
    // nodes should be a copy for isolation
    EXPECT_EQ(nodes.size(), 2);
    const auto nodes2 = nm->nodes();
    EXPECT_EQ(nodes2.size(), 1);
  }

  // node manager for discovery mode - dynamic nodes
  {
    auto nm = NodeManager::create();

    // from node ping - update it to node manager
    nm->update({ NRole::NODE, "128.0.0.1", 9199 });
    nm->update({ NRole::NODE, "128.0.0.1", 9199 });
    nm->update({ NRole::NODE, "128.0.0.2", 9199 });

    // nodes list
    const auto nodes = nm->nodes();
    EXPECT_EQ(nodes.size(), 2);

    // mark a node to be bad
    nm->mark("128.0.0.1:9199");
    // nodes should be a copy for isolation
    EXPECT_EQ(nodes.size(), 2);
    const auto nodes2 = nm->nodes();
    EXPECT_EQ(nodes2.size(), 1);

    // sleep more than healthy_time=3s, all nodes will not be selected
    nebula::common::Evidence::sleep(4000);
    const auto nodes3 = nm->nodes();
    EXPECT_EQ(nodes3.size(), 0);
  }
}

} // namespace test
} // namespace meta
} // namespace nebula
