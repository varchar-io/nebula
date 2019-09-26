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

#include "KafkaReader.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "meta/Table.h"

/**
 * Kafka topic wrapping topic metadata store.
 */
namespace nebula {
namespace storage {
namespace kafka {

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TType;
using apache::thrift::transport::TMemoryBuffer;
using nebula::meta::Table;

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

class SegmentedRebalanceCb : public RdKafka::RebalanceCb {
public:
  SegmentedRebalanceCb(KafkaSegment segment) : segment_{ std::move(segment) } {
  }

public:
  virtual void rebalance_cb(RdKafka::KafkaConsumer* consumer,
                            RdKafka::ErrorCode err,
                            std::vector<RdKafka::TopicPartition*>& partitions) override {

    // pick the partition we want to target
    RdKafka::TopicPartition* target = nullptr;
    for (auto p : partitions) {
      if (p->partition() == segment_.partition) {
        target = p;
        break;
      }
    }

    N_ENSURE_NOT_NULL(target, "target partition not found!!");

    // set offset of this partition
    target->set_offset(segment_.offset);

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      consumer->assign({ target });
    } else {
      consumer->unassign();
    }
  }

private:
  KafkaSegment segment_;
};

// build the subscription pipeline
void KafkaReader::load() {
  // reserve maximum segment size
  messages_.reserve(segment_.size);

  // properties
  const auto& brokers = table_->location;
  const auto& topic = table_->name;

  // set up the kafka configurations
  std::string error;
  auto conf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  auto tconf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

#define SET_KEY_VALUE_CHECK(K, V)                         \
  if (conf->set(K, V, error) != RdKafka::Conf::CONF_OK) { \
    LOG(ERROR) << "Kafka: " << error;                     \
    return;                                               \
  }

  // set up gzip
  SET_KEY_VALUE_CHECK("compression.codec", "gzip")

  // set brokers
  SET_KEY_VALUE_CHECK("metadata.broker.list", brokers)

  // set event callback
  KafkaEventCb ecb;
  SET_KEY_VALUE_CHECK("event_cb", &ecb)

  // set rebalance job
  SegmentedRebalanceCb rcb(segment_);
  SET_KEY_VALUE_CHECK("rebalance_cb", &rcb)

  // group Id is a must
  SET_KEY_VALUE_CHECK("group.id", std::to_string(segment_.partition))

  // set default topic config
  SET_KEY_VALUE_CHECK("default_topic_conf", tconf.get())

#undef SET_KEY_VALUE_CHECK

  auto consumer = std::unique_ptr<RdKafka::KafkaConsumer>(
    RdKafka::KafkaConsumer::create(conf.get(), error));

  // subscribe the topic
  LOG(INFO) << "Consume " << brokers << "/" << topic << ":" << segment_.id();
  RdKafka::ErrorCode err = consumer->subscribe({ topic });
  if (err) {
    LOG(ERROR) << "Failed to subscribe topic: " << topic << ", error: " << RdKafka::err2str(err);
    return;
  }

  // run the loop to process all messages
  size_t count = 0;
  while (count < segment_.size) {
    // when this message is consumed from queue, please delete it
    auto msg = std::unique_ptr<RdKafka::Message>(consumer->consume(timeoutMs_));
    // message may be empty
    if (msg && msg->len() > 0) {
      messages_.push_back(std::move(msg));

      // RdKafka::MessageTimestamp ts = msg->timestamp();
      // counts this valid message
      count++;
    }
  }

  consumer->unsubscribe();

  // TODO(cao): calling close will hanging forever.
  // consumer->close();

  // update current cursor to reflect the size before reader kicks in
  // TODO(cao): blocking approach to load all first, we may go parallel
  this->size_ = messages_.size();
}

// TODO(cao): do we need to handle different timestamp or availability?
inline size_t readMessageTimestamp(const RdKafka::Message& msg) {
  // convert to seconds, nebula use seconds as timestamp
  return (size_t)msg.timestamp().timestamp / 1000;
  // if (ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE)
  // if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
  // if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
}

void KafkaReader::populateThriftBinary(const RdKafka::Message& msg) {
  // reset the flat row
  row_.reset();

  // always write message timestamp into time column
  row_.write(Table::TIME_COLUMN, readMessageTimestamp(msg));

  // start to read
  const auto size = msg.len();
  auto buffer = std::make_shared<TMemoryBuffer>(static_cast<uint8_t*>(msg.payload()), size);
  TBinaryProtocol proto(buffer);

  // field name?
  std::string name;

  // read all fields
  while (true) {
    TType type = apache::thrift::protocol::T_STOP;
    int16_t id = 0;

    proto.readFieldBegin(name, type, id);

    // no more fields to read
    if (id == 0) {
      break;
    }

    // look up which column name this is for
    auto f = fields_.find(id);
    if (f != fields_.end()) {
      auto name = f->second;
#define TYPE_EXTRACT(T, CT, M) \
  case TType::T: {             \
    CT v;                      \
    proto.M(v);                \
    row_.write(name, v);       \
    break;                     \
  }
      switch (type) {
        TYPE_EXTRACT(T_STRING, std::string, readBinary)
        TYPE_EXTRACT(T_BOOL, bool, readBool)
        TYPE_EXTRACT(T_BYTE, int8_t, readByte)
        TYPE_EXTRACT(T_I16, int16_t, readI16)
        TYPE_EXTRACT(T_I32, int32_t, readI32)
        TYPE_EXTRACT(T_I64, int64_t, readI64)
        TYPE_EXTRACT(T_DOUBLE, double, readDouble)

      default: {
        LOG(ERROR) << "Type not supported at field: " << id;
        break;
      }
      }
#undef TYPE_EXTRACT
    } else {
      proto.skip(type);
    }

    proto.readFieldEnd();
  }
}

// next row data of CsvRow
const nebula::surface::RowData& KafkaReader::next() {
  // fetch the message and wrap it into a row data interface
  const auto& msg = messages_.at(this->index_);

  // parse this msg into row_ based on serde info
  if (table_->format == "thrift" && table_->serde.protocol == "binary") {
    populateThriftBinary(*msg);
    index_++;
    return row_;
  }

  throw NException("Only support thrift and TBinaryProtocol for now");
}

} // namespace kafka
} // namespace storage
} // namespace nebula