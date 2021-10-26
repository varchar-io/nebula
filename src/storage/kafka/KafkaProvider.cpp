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

#include "KafkaProvider.h"
#include "KafkaConfig.h"

#include "type/Serde.h"

/**
 * Provide common kafka handles creation.
 */
namespace nebula {
namespace storage {
namespace kafka {

using nebula::common::unordered_map;
using nebula::type::Settings;

// most likely the high memory consumption caused by this
// thread local collection to provide kafka consumer per broker string
// thread_local std::unordered_map<std::string, std::unique_ptr<RdKafka::KafkaConsumer>> consumers;

// Kafka consumer handle is expensive resource which is supposed to reuse
// in the same thread.
std::unique_ptr<RdKafka::KafkaConsumer> KafkaProvider::getConsumer(
  const std::string& brokers, const Settings& settings) {
  // set up the kafka configurations
  std::string error;

  KafkaConfig config{ settings };

#define SET_KEY_VALUE_CHECK(K, V)                         \
  if (conf->set(K, V, error) != RdKafka::Conf::CONF_OK) { \
    LOG(INFO) << "Kafka: " << error;                      \
    return nullptr;                                       \
  }

  // for kafka - the configs are all go to consumers
  auto& conf = config.globalConf;
  auto& tconf = config.topicConf;

  // set brokers
  SET_KEY_VALUE_CHECK("metadata.broker.list", brokers)

  // set group id anyways even we don't use consumer group at all
  SET_KEY_VALUE_CHECK("group.id", "nebula.kafka");

  // const auto INTEGER_MAX = std::to_string(std::numeric_limits<int32_t>::max());
  SET_KEY_VALUE_CHECK("max.poll.interval.ms", "86400000");

  // set event callback by a static instance
  static KafkaEventCb ecb;
  SET_KEY_VALUE_CHECK("event_cb", &ecb)

  // set default topic config
  SET_KEY_VALUE_CHECK("default_topic_conf", tconf.get())

#undef SET_KEY_VALUE_CHECK

  auto ptr = RdKafka::KafkaConsumer::create(conf.get(), error);
  N_ENSURE_NOT_NULL(ptr, error);
  return std::unique_ptr<RdKafka::KafkaConsumer>(ptr);
}

} // namespace kafka
} // namespace storage
} // namespace nebula