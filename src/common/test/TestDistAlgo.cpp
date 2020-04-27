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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/HashRing.h"

/**
 * Test all stats algorithms provided by folly/stats package
 */
namespace nebula {
namespace common {
namespace test {

class Machine final : public nebula::common::Identifiable {
public:
  Machine(const std::string& id) : id_{ id } {}
  ~Machine() = default;
  virtual const std::string& id() const override {
    return id_;
  }

private:
  std::string id_;
};
struct Data final : public nebula::common::Identifiable {
  Data(const std::string& key) : key_{ key } {}
  ~Data() = default;
  std::string key_;
  virtual const std::string& id() const override {
    return key_;
  }
};

// test buidling a hash ring
TEST(HashRingTest, TestHashRingBuild) {
  const auto CLUSTER_SIZE = 100;

  std::vector<std::unique_ptr<Machine>> machines;
  machines.reserve(CLUSTER_SIZE);
  for (auto i = 0; i < CLUSTER_SIZE; ++i) {
    machines.push_back(std::make_unique<Machine>(fmt::format("NODE-{0}", i)));
  }

  // build a ring out this machine list
  HashRing<Machine, Data, CLUSTER_SIZE> ring(machines);
  ring.print();
}

// test buidling a hash ring
TEST(HashRingTest, TestDataDistribution) {
  const auto CLUSTER_SIZE = 20;

  std::vector<std::unique_ptr<Machine>> machines;
  machines.reserve(CLUSTER_SIZE);
  for (auto i = 0; i < CLUSTER_SIZE; ++i) {
    machines.push_back(std::make_unique<Machine>(fmt::format("NODE-{0}", i)));
  }

  // prepare 10000 data keys
  const auto DATA_ITEMS = 1000000;
  std::vector<Data> data;
  data.reserve(DATA_ITEMS);
  for (auto i = 0; i < DATA_ITEMS; ++i) {
    data.emplace_back(fmt::format("DATA-{0}", i));
  }

  // build a ring out this machine list
  HashRing<Machine, Data, CLUSTER_SIZE> ring(machines);
  ring.print();

  // assign all these data into the ring
  for (auto& d : data) {
    ring.attach(d);
  }

  for (auto i = 0; i < DATA_ITEMS; ++i) {
    if (i % 5 == 0) {
      ring.detach(data.at(i));
    }
  }

  ring.print();
}

// test buidling a hash ring
TEST(HashRingTest, TestDataReblanceWithNewNode) {
  const auto CLUSTER_SIZE = 20;

  std::vector<std::unique_ptr<Machine>> machines;
  machines.reserve(CLUSTER_SIZE);
  for (auto i = 0; i < CLUSTER_SIZE; ++i) {
    machines.push_back(std::make_unique<Machine>(fmt::format("NODE-{0}", i)));
  }

  // prepare 10000 data keys
  const auto DATA_ITEMS = 1000;
  std::vector<Data> data;
  data.reserve(DATA_ITEMS);
  for (auto i = 0; i < DATA_ITEMS; ++i) {
    data.emplace_back(fmt::format("DATA-{0}", i));
  }

  // build a ring out this machine list
  HashRing<Machine, Data, CLUSTER_SIZE> ring(machines);

  // assign all these data into the ring
  for (auto& d : data) {
    ring.attach(d);
  }

  ring.print();

  // add a new node and expect it to have some data balanced
  auto node21 = std::make_unique<Machine>("NODE-21");
  ring.add(std::move(node21));
  ring.print();

  auto node22 = std::make_unique<Machine>("NODE-22");
  ring.add(std::move(node22));
  ring.print();
}

TEST(HashRingTest, TestAddOneNodeEachTime) {
  std::vector<std::unique_ptr<Machine>> init;
  init.reserve(1);
  init.push_back(std::make_unique<Machine>("NODE-1"));
  HashRing<Machine, Data, 20> ring(init);

  // attach 1K data
  const auto DATA_ITEMS = 1000;
  for (auto i = 0; i < DATA_ITEMS; ++i) {
    Data d{ fmt::format("DATA-{0}", i) };
    ring.attach(d);
  }

  // add 19 nodes one by one
  for (auto i = 2; i < 21; ++i) {
    auto node = std::make_unique<Machine>(fmt::format("NODE-{0}", i));
    ring.add(std::move(node));

    EXPECT_EQ(ring.numNodes(), i);
    EXPECT_EQ(ring.numKeys(), DATA_ITEMS);
    ring.print();
  }

  // remove 19 nodes one by one
  for (auto i = 2; i < 21; ++i) {
    Machine node(fmt::format("NODE-{0}", i));
    ring.remove(node);

    EXPECT_EQ(ring.numNodes(), 21 - i);
    EXPECT_EQ(ring.numKeys(), DATA_ITEMS);
    ring.print();
  }
}

} // namespace test
} // namespace common
} // namespace nebula