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

#include <forward_list>
#include <glog/logging.h>
#include <mutex>
#include <numeric>

#include "Errors.h"
#include "Finally.h"
#include "Hash.h"
#include "Identifiable.h"
#include "Likely.h"
#include "Wrap.h"

namespace nebula {
namespace common {

// An implementaton of consistent hash
// wiki ref: https://en.wikipedia.org/wiki/Consistent_hashing
//
// This is another way to build a hash space based on prime space for better distribution
// However it is probably more difficult to make domain specific change to fit Nebula case.
//
// In current implementaton, we will support
// 1. Basic ring with even distribution with labels (or virtual nodes whatever naming scheme).
// 2. Micro tune capability to set weight for different node by adjusting number of hash.
// 3. Template the algorithm so that it can be plug in different data structure easily.

// represents a resource node on the ring
template <typename NT,
          size_t M,
          size_t H,
          typename = std::enable_if_t<std::is_base_of_v<Identifiable, NT>>>
class RingNode {
public:
  RingNode(std::unique_ptr<NT> resource) : resource_{ std::move(resource) } {
    // generate H placement for this given node
    const auto& key = resourceId();
    placements_ = Hasher::hash64<H>(key.data(), key.size());

    // map all the hash values into the defined space [0, M)
    std::transform(placements_.begin(), placements_.end(), placements_.begin(), [](auto x) {
      return x % M;
    });
  }
  virtual ~RingNode() = default;

public:
  inline const std::array<size_t, H>& placements() const {
    return placements_;
  }

  inline const NT& resource() const {
    return *resource_;
  }

  // a shortcut method to resource().id()
  inline const std::string& resourceId() const {
    return resource_->id();
  }

  inline const unordered_set<std::string>& keys() const {
    return keys_;
  }

  inline void addKey(const std::string& key) {
    // add a key to this node
    keys_.emplace(key);
  }

  inline void removeKey(const std::string& key) {
    // add a key to this node
    auto x = keys_.erase(key);
    if (x != 1) {
      LOG(INFO) << "key not found: " << key;
    }
    N_ENSURE_EQ(x, 1, "exact one key removed");
  }

  bool operator==(const RingNode& another) {
    return resourceId() == another.resourceId();
  }

private:
  // represents the resource on the ring.
  // in the context of cluster management, it represents a node/host
  std::unique_ptr<NT> resource_;

  // every ring node could have multiple virtual nodes
  // which can be called "labels", or "placements" on the ring
  std::array<size_t, H> placements_;

  // an hash set to contain all data attached to this node
  unordered_set<std::string> keys_;
};

// reverse index from placement to RingNode
// Since one RingNode could have H placements (labels)ss
template <typename RN>
struct Placement final {
  explicit Placement(size_t h, RN* n)
    : hash{ h }, node{ n } {}
  ~Placement() = default;

  // hash value of this placement
  size_t hash;

  // the raw pointer to the node it belongs to
  // Note: why pointer here but not reference?
  // because if Placement is used as vector member, sorting on the container will require
  // the object is move constructable, hence every member of this object needs to be move construtable too.
  // Reference type doesn't have this property while raw pointer is just an integer essentially, less safer though.
  // We may consider using weak_ptr instead of raw pointer.
  RN* node;
};

// side note: many facinating ways https://en.cppreference.com/w/cpp/types/enable_if
// NT: represents NODE type or resource type
// DT: represents DATA type, it is usually the key type to be distributed
// S: cluster size - this implementation doesn't support cluster size larger than 1000
template <typename NT,
          typename DT,
          size_t S = 20,
          typename = std::enable_if_t<(S <= 1000) && std::is_base_of_v<Identifiable, NT> && std::is_base_of_v<Identifiable, DT>>>
class HashRing final {
  // ring max to define the ring space [0, MAX)
  // The MAX is a prime number.
  static constexpr size_t RING_MAX = 1235711;

  // S denotes cluster size, we use simple 1K virtual nodes to decide H value
  // TODO(cao): this doesn't look scientific, looking a better way to make a more even distributed ring.
  static constexpr size_t H = 1000 / S;

  // RingNode Type
  using TRingNode = RingNode<NT, RING_MAX, H>;
  // Placement Type
  using TPlacement = Placement<TRingNode>;

// this macro will lock data change for consistency
#define DATA_LOCK const std::lock_guard<std::mutex> lock(mdata_);

public:
  HashRing(std::vector<std::unique_ptr<NT>>& nodes) {
    // reserve 1.2x slots for node placements
    const auto numNodes = nodes.size();
    const auto numPlacements = numNodes * H;
    nebula::common::vector_reserve(placements_, numPlacements + numPlacements / 5, "HashRing::HashRing");

    // initialize the ring by create all placements based on the node set
    // every single node will be mapped into a ring node
    for (auto& obj : nodes) {
      // create a placement
      add(std::make_unique<TRingNode>(std::move(obj)));
    }

    // keep placements in sorting
    sort();
  }
  ~HashRing() = default;

public:
  // add a new node to the hash ring and return number of placements
  size_t add(std::unique_ptr<NT> node) {
    // a new ring node
    auto rn = std::make_unique<TRingNode>(std::move(node));

    // for every single placement, we will need to move keys around
    {
      // LOCK data operation in current scope {} and sort at the end
      DATA_LOCK
      Finally f([this]() { sort(); });

      auto high = placements_.back().hash;
      auto end = placements_.size();
      for (auto& h : rn->placements()) {
        // core logic to move impacted data around
        // (...[i-1] ... [i] ...)
        auto i = place(h, end);
        auto& p = placements_.at(i);
        // hash collision, we ignore this placement, allowing pre-existing node to take data
        if (h == p.hash) {
          continue;
        }

        N_ENSURE(h < p.hash || h > high, "new placement will split the space.");
        // TODO(cao) - we don't associate data with placement, instead under node directly
        // 2 sides:
        //    1) placement can be super light to be flexiblely add/remove (e.g weight adjustment).
        //    2) when moving data, we have to check all data in the nodes, a bit performance loss.
        const auto low = i == 0 ? high : placements_.at(i - 1).hash;
        std::forward_list<std::string> keys;
        for (auto& key : p.node->keys()) {
          // now if the key's hash
          auto kh = hash(key);
          if (kh <= h && kh > low) {
            keys.push_front(key);
          }
        }

        // need another loop to modify p.node->keys
        for (auto& key : keys) {
          // move this key from p.node to current node
          p.node->removeKey(key);
          rn->addKey(key);
        }

        // after all keys moved, let's officially get this placement online
        placements_.emplace_back(h, rn.get());
      }
    }

    // move this node into node list
    auto numPlacements = rn->placements().size();
    nodes_.emplace(std::move(rn));
    return numPlacements;
  }

  // remove an existing node from the hash ring and return number of placements removed
  size_t remove(const NT& node) {
    // locate this node, and remove all placements from the ring
    // build a node target for hash find purpose
    for (auto& n : nodes_) {
      if (n->resourceId() == node.id()) {
        // remove all placements of this node
        {
          DATA_LOCK
          for (auto& h : n->placements()) {
            // search in placements_
            auto itr = std::find_if(placements_.begin(), placements_.end(), [&h, &n](const auto& placement) {
              return placement.hash == h && placement.node == n.get();
            });

            // it's possible we can not find the placement or placement belongs to other node due to hash collision
            if (itr != placements_.end()) {
              placements_.erase(itr);
            }
          }
        }

        // placements all erased - since it's sorted, we don't need to sort again
        // so now, let's assign all data keys of this node to existing ring
        // here is another performance hicup due to placement doesn't associate with data.
        // otherwise, we can just move its belongings to next placement in the ring/chain
        // without this reassignment
        for (auto& key : n->keys()) {
          DT temp{ key };
          attach(temp);
        }

        // assign all keys to existing ring
        return nodes_.erase(n);
      }
    }

    return 0;
  }

// Note: we don't manage DT object internally like NT, instead we only cache data key
// because we think HashRing is used for resource management rather than data management.
// Hence HashRing API should be enough for external data management system.
#define DATA_OP(METHOD, PROXY)                  \
  const NT& METHOD(const DT& data) {            \
    DATA_LOCK                                   \
    const auto& key = data.id();                \
    const auto h = hash(key);                   \
    auto placement = place(h);                  \
    auto node = placements_.at(placement).node; \
    node->PROXY(key);                           \
    return node->resource();                    \
  }

  // attach data into the ring, it will attach the key to specific node in return
  DATA_OP(attach, addKey)

  // remove data from the ring, it will locate the node and remove resource from it
  DATA_OP(detach, removeKey)

#undef DATA_OP

  inline size_t numNodes() const {
    return nodes_.size();
  }

  inline size_t numKeys() const {
    return std::accumulate(nodes_.begin(), nodes_.end(), 0, [](size_t value, const auto& node) {
      return value + node->keys().size();
    });
  }

  // print out the ring
  void print() {
    const auto numPlacements = placements_.size();
    const auto numNodes = nodes_.size();
    // check size, a node can have one or more placements
    // a smart ring will adjust number of a node's placements based on its weight (such as capcity)
    // so placements size should larger or equal node size
    N_ENSURE(numPlacements >= numNodes, "Relation check between placements and nodes.");
    LOG(INFO) << "Hash Ring: " << H
              << " Nodes: " << numNodes
              << " Placements: " << numPlacements
              << " Keys: " << numKeys();

    // print placements in order
    size_t last = 0;
    auto duplicates = 0;
    for (auto& p : placements_) {
      VLOG(1) << "Placement: hash=" << p.hash << ", node=" << p.node->resourceId();
      N_ENSURE(p.hash >= last, "all placements should be sorted in order");
      if (p.hash == last) {
        ++duplicates;
      }
      last = p.hash;
    }
    LOG(INFO) << "Duplicate placements: " << duplicates;

    // print all data keys in each node
    auto cover = coverage();
    for (auto& n : nodes_) {
      LOG(INFO) << "Node: " << n->resourceId() << ", Keys: " << n->keys().size() << ", Area: " << cover[n->resourceId()];
      for (auto& key : n->keys()) {
        VLOG(1) << "    Key: " << key;
      }
    }
  }

  // report coverage for each node
  inline unordered_map<std::string, double> coverage() const {
    // go through every gap and attribute the space to each node
    unordered_map<std::string, double> result;
    auto accumulator = [max = RING_MAX](unordered_map<std::string, double>& map, std::string& node, std::pair<size_t, size_t>& range) {
      auto distance = range.first < range.second ?
                        (range.second - range.first) :
                        (max - range.first + range.second);
      auto kv = map.find(node);
      if (kv != map.end()) {
        kv->second += distance;
      } else {
        map.emplace(node, distance);
      }
    };

    for (size_t i = 0, size = placements_.size(); i < size; ++i) {
      auto r = coverage(i);
      auto n = placements_.at(i).node->resourceId();
      accumulator(result, n, r);
    }

    // translate the value into a ratio
    std::for_each(result.begin(), result.end(), [](auto& item) {
      item.second /= RING_MAX;
    });

    return result;
  }

private:
  // get coverage node in format of (start, end]
  inline std::pair<size_t, size_t> coverage(size_t placementIndex) const {
    if (N_UNLIKELY(placementIndex == 0)) {
      return std::make_pair(placements_.back().hash, placements_.front().hash);
    }

    return std::make_pair(placements_.at(placementIndex - 1).hash, placements_.at(placementIndex).hash);
  }

  // add a new ring node to the ring
  size_t add(std::unique_ptr<TRingNode> rn) {
    const auto numPlacements = rn->placements().size();
    // add all placements of this ring node
    // TODO(cao): handle hash collision if two placements share the same hash value
    // Revisit: it seems okay for collison across nodes, single node gurantees no duplicates.
    for (auto& hash : rn->placements()) {
      placements_.emplace_back(hash, rn.get());
    }

    // add this node to node repo
    nodes_.emplace(std::move(rn));
    return numPlacements;
  }

  // all key hash will be from here
  inline size_t hash(const std::string& key) const {
    return Hasher::hash64(key.data(), key.size()) % RING_MAX;
  }

  // find the proper node to host given key hash
  // if end is passed in, it means we only apply search in space [0, end)
  size_t place(size_t hash, size_t end = 0) const {
    if (end == 0) {
      end = placements_.size();
    }

    const auto last = end - 1;
    // search space to host this hash value
    if (hash > placements_.at(last).hash || hash <= placements_.front().hash) {
      return 0;
    }

    // apply binary search
    size_t left = 0, right = last;
    while (left < right) {
      auto mid = (left + right) / 2;
      auto range = coverage(mid);
      if (hash > range.first && hash <= range.second) {
        return mid;
      }

      if (hash > range.second) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    // actually it should not come to here, throw?
    return left;
  }

  // sort placements in the ring
  void sort() {
    std::sort(placements_.begin(), placements_.end(), [](const auto& s1, const auto& s2) -> bool {
      return s1.hash < s2.hash;
    });
  }

private:
  // data operation mutex, such as moving key around, add keys, remove keys, etc.
  std::mutex mdata_;
  // this is repository of all nodes
  unordered_set<std::unique_ptr<TRingNode>> nodes_;
  // all placements in the ring, it maintains a sorted metadata for all placements
  std::vector<TPlacement> placements_;

#undef DATA_LOCK
};

} // namespace common
} // namespace nebula