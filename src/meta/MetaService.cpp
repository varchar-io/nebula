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

#include "MetaService.h"

/**
 * Node manager managing all nebula working nodes
 */
namespace nebula {
namespace meta {

// update data specs to table registry
// table registry could persist these states in external store
void TableRegistry::update(const std::string& version, const std::vector<SpecPtr>& specs) noexcept {
  if (expired()) {
    onlineSpecs_.clear();
    return;
  }

  // next version of all specs
  nebula::common::unordered_map<std::string, SpecPtr> snapshot;
  snapshot.reserve(specs.size());

  // go through the new spec list and update the existing ones
  // need lock here?
  auto created = 0;
  auto renewed = 0;
  for (auto itr = specs.cbegin(), end = specs.cend(); itr != end; ++itr) {
    // check if we have this spec already?
    auto specPtr = (*itr);
    const auto& sign = specPtr->id();
    auto found = onlineSpecs_.find(sign);
    if (found != onlineSpecs_.end()) {
      auto prev = found->second;
      // by default, we carry over existing spec's properties
      const auto& node = prev->affinity();
      specPtr->affinity(node);
      specPtr->state(prev->state());

      // if the node is not active, reassign
      if (!node.isActive()) {
        specPtr->affinity(NNode::invalid());
        specPtr->state(SpecState::NEW);
        renewed++;
      }
    } else {
      created++;
    }

    // move to the next version
    snapshot.emplace(sign, specPtr);
  }

  // print out update stats
  if (created > 0 || renewed > 0) {
    LOG(INFO) << "Updating specs for " << table_->name()
              << ": created=" << created
              << ", renewed=" << renewed
              << ", count=" << snapshot.size();

    // let's swap with existing one
    if (specs.size() != snapshot.size()) {
      LOG(WARNING) << "Duplicate specs found.";
    }

    // swap the new snapshot in
    this->version_ = version;
    std::swap(onlineSpecs_, snapshot);
  }
}

size_t MetaService::incrementQueryServed() noexcept {
  auto& db = metadb();

  std::string value;
  if (queryCount_ == 0 && db.read(QUERY_COUNT, value)) {
    queryCount_ = folly::to<size_t>(value);
  }
  value = std::to_string(++queryCount_);
  db.write(QUERY_COUNT, value);

  return queryCount_;
}

// short a URL into a 6-letter code
std::string MetaService::shortenUrl(const std::string& url) noexcept {
  static constexpr auto MAX_ATTEMPT = 5;
  auto& db = metadb();
  // handle collision
  auto str = url.data();
  auto size = url.size();
  auto i = 0;
  while (i++ < MAX_ATTEMPT) {
    // get digest
    auto digest = nebula::common::Chars::digest(str, size);
    // if the digest does not exist, we save it and return
    std::string value;
    if (db.read(digest, value)) {
      // same URL already existing
      if (value == url) {
        return digest;
      }

      // not the same, we have collision, change input and try it again
      size -= 6;
      continue;
    }

    // key is not existing, write it out
    db.write(digest, url);
    return digest;
  }

  // nothing is working
  return {};
}

} // namespace meta
} // namespace nebula