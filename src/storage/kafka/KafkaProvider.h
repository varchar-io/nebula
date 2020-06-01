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

#include <glog/logging.h>
#include <rdkafkacpp.h>
#include <unordered_map>

#include "common/Errors.h"

/**
 * Provide common kafka handles creation.
 */
namespace nebula {
namespace storage {
namespace kafka {

class KafkaEventCb : public RdKafka::EventCb {
public:
  virtual void event_cb(RdKafka::Event& event) override {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      if (event.fatal()) {
        LOG(FATAL) << "event error";
      }
      LOG(ERROR) << "ERROR (" << RdKafka::err2str(event.err()) << "): " << event.str();
      break;

    case RdKafka::Event::EVENT_STATS:
      // LOG(ERROR) << "\"STATS\": " << event.str();
      break;

    case RdKafka::Event::EVENT_LOG:
      LOG(ERROR) << fmt::format("LOG-{0}-{1}: {2}", event.severity(), event.fac().c_str(), event.str().c_str());
      break;

    case RdKafka::Event::EVENT_THROTTLE:
      LOG(ERROR) << "THROTTLED: " << event.throttle_time() << "ms by " << event.broker_name() << " id " << (int)event.broker_id();
      break;

    default:
      LOG(ERROR) << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): " << event.str();
      break;
    }
  }
};

class KafkaProvider {
public:
  // Kafka consumer handle is expensive resource which is supposed to reuse
  // in the same thread.
  static std::unique_ptr<RdKafka::KafkaConsumer> getConsumer(const std::string&, const std::unordered_map<std::string, std::string>&);
};

} // namespace kafka
} // namespace storage
} // namespace nebula