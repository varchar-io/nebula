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
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "common/Evidence.h"
#include "meta/Table.h"

/**
 * Kafka topic wrapping topic metadata store.
 */
namespace nebula {
namespace storage {
namespace kafka {

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::protocol::TType;
using apache::thrift::transport::TMemoryBuffer;
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
void KafkaReader::load(RdKafka::KafkaConsumer* consumer) {
  // reserve maximum segment size
  messages_.reserve(segment_.size);

  // properties
  const auto& topic = table_->name;

  // subscribe is designed for group balance, we use assign directly
  LOG(INFO) << "Consume " << table_->location << "/" << topic << ":" << segment_.id();
  auto p = std::unique_ptr<RdKafka::TopicPartition>(
    RdKafka::TopicPartition::create(topic, segment_.partition, timeoutMs_));

  // set offset of this partition
  p->set_offset(segment_.offset);

  // assign the partition to start consuming
  consumer->assign({ p.get() });

  // run the loop to process all messages
  // TODO(cao) - due kafka version without timestamp.
  // split may not have precise offset info
  const int64_t max = segment_.offset + segment_.size;
  size_t numErrors = 0;
  while (messages_.size() < segment_.size) {
    // when this message is consumed from queue, please delete it
    auto msg = std::unique_ptr<RdKafka::Message>(consumer->consume(timeoutMs_));

    // message may be empty
    if (msg && msg->len() > 0) {
      // check if the message has error
      if (msg->err() != RdKafka::ERR_NO_ERROR) {
        LOG(ERROR) << "Error in reading kafka message: " << msg->errstr();

        // more than half messages are error, not waiting any more
        if (numErrors++ >= segment_.size / 5) {
          LOG(ERROR) << "More than 20% messages are error, give up...";
          break;
        }
      }

      auto offset = msg->offset();
      messages_.push_back(std::move(msg));

      // for unpredictable size
      if (offset >= max) {
        break;
      }
    }
  }

  // unassign the partition
  consumer->unassign();
  // consumer->unsubscribe();
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

constexpr auto LEVEL = 1'000;

void readStruct(uint64_t base,
                TBinaryProtocol& proto,
                std::unordered_map<uint32_t, std::string>& fields,
                std::unordered_set<uint32_t>& fieldsWritten,
                FlatRow& row) {
  // field name?
  std::string name;

  while (true) {
    TType type = apache::thrift::protocol::T_STOP;
    int16_t id = 0;

    proto.readFieldBegin(name, type, id);

    // no more fields to read
    if (id == 0) {
      proto.skip(type);
      proto.readFieldEnd();
      break;
    }

    // adjustment based on base
    auto levelId = id + base;

    // look up which column name this is for
    // TODO(cao): I don't know why the same ID will hit twice here
    // let use the first one before figuring out the reason
    auto w = fieldsWritten.find(levelId);
    auto f = fields.find(levelId);

    // if this is not written yet and it's a desired field
    if (w == fieldsWritten.end() && f != fields.end()) {
      auto name = f->second;

      // time field special handling
      if (UNLIKELY(name == Table::TIME_COLUMN)) {
        int64_t time = Evidence::unix_timestamp();
        proto.readI64(time);
        row.write(name, Evidence::to_seconds(time));
        fieldsWritten.emplace(levelId);
        proto.readFieldEnd();
        continue;
      }

#define TYPE_EXTRACT(T, CT, M)      \
  case TType::T: {                  \
    CT v;                           \
    proto.M(v);                     \
    row.write(name, v);             \
    fieldsWritten.emplace(levelId); \
    break;                          \
  }

      switch (type) {
        TYPE_EXTRACT(T_STRING, std::string, readBinary)
        TYPE_EXTRACT(T_BOOL, bool, readBool)
        TYPE_EXTRACT(T_BYTE, int8_t, readByte)
        TYPE_EXTRACT(T_I16, int16_t, readI16)
        TYPE_EXTRACT(T_I32, int32_t, readI32)
        TYPE_EXTRACT(T_I64, int64_t, readI64)
        TYPE_EXTRACT(T_DOUBLE, double, readDouble)
      case TType::T_LIST: {
        // TODO(cao): we only support size of list for now
        TType elemType = apache::thrift::protocol::T_STOP;
        uint32_t listSize;
        proto.readListBegin(elemType, listSize);
        row.write(name, listSize);
        fieldsWritten.emplace(levelId);
        proto.readListEnd();
        break;
      }
      default: {
        VLOG(1) << "Type not supported at field: " << levelId << " with type=" << type;
        proto.skip(type);
        break;
      }
      }
#undef TYPE_EXTRACT
    } else if (type == TType::T_STRUCT) {
      // support simple nesting
      proto.readStructBegin(name);
      // recursively
      readStruct(levelId * LEVEL, proto, fields, fieldsWritten, row);
      proto.readStructEnd();
    } else {
      proto.skip(type);
    }

    proto.readFieldEnd();
  }
}

void KafkaReader::populateThriftBinary(const RdKafka::Message& msg) {
  // reset the flat row
  row_.reset();

  // always write message timestamp into time column
  if (!includeTime_) {
    row_.write(Table::TIME_COLUMN, readMessageTimestamp(msg));
  }

  // start to read
  const auto size = msg.len();
  auto buffer = std::make_shared<TMemoryBuffer>(static_cast<uint8_t*>(msg.payload()), size);
  TBinaryProtocol proto(buffer);

  // read all fields
  const auto numFields = fields_.size();
  std::unordered_set<uint32_t> fieldsWritten;
  fieldsWritten.reserve(numFields);

  // TODO(cao): ID path hack, every time it enter into a new level, it times 10K to get next field ID
  // current field;
  readStruct(0, proto, fields_, fieldsWritten, row_);

  // in case anything happened, not all fields found from this message
  const auto numWritten = fieldsWritten.size();
  if (UNLIKELY(numWritten < numFields)) {
    for (auto itr = fields_.cbegin(); itr != fields_.cend(); ++itr) {
      if (fieldsWritten.find(itr->first) == fieldsWritten.end()) {
        row_.writeNull(itr->second);
      }
    }
  }
}

// next row data of CsvRow
const nebula::surface::RowData& KafkaReader::next() {
  // fetch the message and wrap it into a row data interface
  const auto& msg = messages_.at(this->index_);

  // parse this msg into row_ based on serde info
  if (table_->format == "thrift" && table_->serde.protocol == "binary") {
    try {
      populateThriftBinary(*msg);
    } catch (std::exception& exp) {
      LOG(ERROR) << "Error in parsing a message at offset=" << msg->offset() << ": " << exp.what();
      nullifyRow();
    }

    index_++;
    return row_;
  }

  throw NException("Only support thrift and TBinaryProtocol for now");
}

} // namespace kafka
} // namespace storage
} // namespace nebula