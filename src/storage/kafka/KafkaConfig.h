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

#include <glog/logging.h>
#include <rdkafkacpp.h>
#include <string>
#include <unordered_map>

#include "common/Chars.h"
#include "common/Errors.h"

/**
 * Kafka topic wrapping topic metadata store.
 */
namespace nebula {
namespace storage {
namespace kafka {

// provide a way to initialize kafka configs
// it supports user settings through "kafka." prefix overwrite
struct KafkaConfig {
  static constexpr std::string_view KAFKA_PFX = "kafka.";
  static constexpr size_t KAFKA_PFX_LEN = KAFKA_PFX.size();
  static bool setAndCheck(
    std::unique_ptr<RdKafka::Conf>& conf,
    const std::string& key,
    const std::string& value) noexcept {
    std::string error;
    if (conf->set(key, value, error) != RdKafka::Conf::CONF_OK) {
      LOG(INFO) << "Kafka config set error: " << error;
      return false;
    }

    return true;
  }

  // members for global and topic respectively
  std::unique_ptr<RdKafka::Conf> globalConf;
  std::unique_ptr<RdKafka::Conf> topicConf;

  // initialize a kafka config object
  explicit KafkaConfig(const std::unordered_map<std::string, std::string>& settings)
    : globalConf{ RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL) },
      topicConf{ RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC) } {

    // apply user settings
    for (auto itr = settings.begin(); itr != settings.end(); ++itr) {
      const auto& key = itr->first;
      if (nebula::common::Chars::prefix(key.data(), key.size(), KAFKA_PFX.data(), KAFKA_PFX_LEN)) {
        std::string realKey(key.data() + KAFKA_PFX_LEN, key.size() - KAFKA_PFX_LEN);
        VLOG(1) << "Set kafka config from user settings: " << realKey;
        setAndCheck(globalConf, realKey, itr->second);
      }
    }
  }
};

} // namespace kafka
} // namespace storage
} // namespace nebula