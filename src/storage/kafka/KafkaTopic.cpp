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

#include "KafkaTopic.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

/**
 * Kafka topic wrapping topic metadata store.
 */
namespace nebula {
namespace storage {
namespace kafka {
bool KafkaTopic::init() noexcept {
  // set up the kafka configurations
  std::string error;
  conf_ = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  tconf_ = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

#define SET_KEY_VALUE_CHECK(K, V)                            \
  if (conf_->set(#K, #V, error) != RdKafka::Conf::CONF_OK) { \
    LOG(ERROR) << "Kafka: " << error;                        \
    return false;                                            \
  }

  // set up gzip
  SET_KEY_VALUE_CHECK(compression.codec, gzip)

  // set brokers
  conf_->set("metadata.broker.list", brokers_, error);

  // group Id is a must
  SET_KEY_VALUE_CHECK(group.id, 0)

  // set default topic config
  conf_->set("default_topic_conf", tconf_.get(), error);

#undef SET_KEY_VALUE_CHECK
  return true;
}

// based on the start time, return a kafka segment list
// this function will generate segments for each partition given start time stamp and segment width
// it is querying the start and end offset by the time condition of each partition
// and figure out "STARTING point" of each segment by width (W) in the line of
//  [0, W) [W, 2W) [2W, 3W) .... [NW, (N+1)W)
std::list<KafkaSegment> KafkaTopic::segmentsByTimestamp(size_t timeMs, size_t width) noexcept {
  std::list<KafkaSegment> segments;
  std::string error;

  // create a consumer
  auto consumer = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(conf_.get(), error));
  if (!consumer) {
    LOG(ERROR) << "Kafka: " << error;
    return segments;
  }

  // figure out partition count
  auto topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(consumer.get(), topic_, tconf_.get(), error));
  if (!topic) {
    LOG(ERROR) << "Kafka: " << error;
    return segments;
  }

  // fetch metadata
  RdKafka::Metadata* metadata;
  if (consumer->metadata(false, topic.get(), &metadata, timeoutMs_) != RdKafka::ERR_NO_ERROR) {
    LOG(ERROR) << "Kafka: can not fetch metadata";
    return segments;
  }

  // save metadata info
  auto partitions = metadata->topics()->at(0)->partitions();
  if (partitions->size() == 0) {
    LOG(ERROR) << "Kafka: topic has no partitions.";
    return segments;
  }

  std::vector<int32_t> pids;
  pids.reserve(partitions->size());
  std::transform(partitions->cbegin(), partitions->cend(),
                 std::back_insert_iterator(pids),
                 [](auto p) {
                   return p->id();
                 });

  // delete metadata object and return
  delete metadata;

  for (auto part : pids) {
    // create partition pointing to a specific time stamp
    auto p = RdKafka::TopicPartition::create(topic_, part, timeMs);
    std::vector<RdKafka::TopicPartition*> ps{ p };
    consumer->assign(ps);
    consumer->offsetsForTimes(ps, timeoutMs_);

    // part_list_print(consumer, p);
    auto startOffset = p->offset();
    consumer->unassign();
    delete p;

    // get high end
    int64_t lowOffset = -1;
    int64_t highOffset = -1;
    if (consumer->query_watermark_offsets(
          topic_, part, &lowOffset, &highOffset, timeoutMs_)
        != RdKafka::ERR_NO_ERROR) {
      LOG(ERROR) << "Kafka: failed to query watermark offsets.";
      continue;
    }

    // we use this range [startOffset, highOffset] to pick the latest range [start, end)
    // any future updates will cover uncovered ranges
    auto start = startOffset / width;
    auto end = highOffset / width;
    while (start < end) {
      segments.emplace_back(part, width * start++, width);
    }
  }

  // close the consumer
  consumer->close();

  return segments;
}

} // namespace kafka
} // namespace storage
} // namespace nebula