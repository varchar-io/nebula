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

#include "KafkaReader.h"

// we know TCompactProtocol will define LIKELY AND UNLIKELY
#ifdef UNLIKELY
#undef UNLIKELY
#endif

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/Evidence.h"
#include "meta/Table.h"

/**
 * Kafka topic wrapping topic metadata store.
 */
namespace nebula {
namespace storage {
namespace kafka {

using nebula::common::Evidence;
using nebula::memory::FlatRow;
using nebula::meta::Table;

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
void KafkaReader::init() {
  // properties
  const auto& topic = table_->name;

  // subscribe is designed for group balance, we use assign directly
  LOG(INFO) << "Consume " << table_->location << "/" << topic << ":" << segment_.id();
  partition_ = std::unique_ptr<RdKafka::TopicPartition>(
    RdKafka::TopicPartition::create(topic, segment_.partition, timeoutMs_));

  auto offset = segment_.offset;

  // assign the partition to start consuming
  consumer_ = KafkaProvider::getConsumer(table_->location, table_->settings);

  // a segment can have offset even smaller than valid range of the partition
  // due to range chunking, adjust partition offset if this is the case
  int64_t lowOffset = -1;
  int64_t highOffset = -1;
  if (consumer_->query_watermark_offsets(
        topic, segment_.partition, &lowOffset, &highOffset, timeoutMs_)
        == RdKafka::ERR_NO_ERROR
      && lowOffset > offset) {
    offset = lowOffset;
    LOG(INFO) << "Adjust partition offset to low bound.";
  }

  // set partition offset to read and assign to current consumer
  partition_->set_offset(offset);
  consumer_->assign({ partition_.get() });

  // create parser
  // support thrift binary and json
  if (table_->format == "thrift" && table_->serde.protocol == "binary") {
    parser_ = std::make_unique<ThriftRow>(table_->serde.cmap);
  } else if (table_->format == "json") {
    parser_ = std::make_unique<JsonRow>(nebula::type::TypeSerializer::from(table_->schema));
  } else {
    throw NException("Only support thrift(TBinaryProtocol) and JSON for now.");
  }

  // set errors to 0 and set maximum messages to load
  errors_ = 0;
  max_ = segment_.offset + segment_.size;

  // load the first message
  msg_ = message();
}

std::unique_ptr<RdKafka::Message> KafkaReader::message() {
  // no more message as we have served requested size
  if (this->size_ >= segment_.size) {
    return nullptr;
  }

  // when this message is consumed from queue, please delete it
  std::unique_ptr<RdKafka::Message> msg(consumer_->consume(timeoutMs_));

  // message may be empty
  if (msg && msg->len() > 0) {
    // check if the message has error
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      LOG(ERROR) << "Error in reading kafka message: " << msg->errstr();

      // more than half messages are error, not waiting any more
      if (errors_++ >= segment_.size / 5) {
        LOG(ERROR) << "More than 20% messages are error, give up...";
        return nullptr;
      }
    }

    // for unpredictable element beyond max limit
    if (msg->offset() > max_) {
      return nullptr;
    }
  }

  ++this->size_;
  return msg;
}

// next row data of CsvRow
const nebula::surface::RowData& KafkaReader::next() {
  // parse this msg into row_ based on serde info
  // reset the flat row
  row_.reset();

  // always write message timestamp into time column
  const size_t msgSize = msg_->len();
  const size_t msgTime = msg_->timestamp().timestamp / 1000;

  // message is empty or failed in parsing its payload
  if (msgSize == 0
      || !parser_->parse(msg_->payload(), msgSize, row_)) {
    LOG(WARNING) << "Invalid kafka message in parsing, size=" << msgSize;
    parser_->nullify(row_, msgTime);
  } else if (!parser_->hasTime()) {
    row_.write(Table::TIME_COLUMN, msgTime);
  }

  // move index and load next message
  index_++;
  msg_ = message();

  // return the parsed row object
  return row_;
}

} // namespace kafka
} // namespace storage
} // namespace nebula