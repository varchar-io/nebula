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

#include "KafkaProvider.h"
#include "KafkaTopic.h"

#include "common/Errors.h"
#include "memory/FlatRow.h"
#include "meta/TableSpec.h"
#include "storage/JsonReader.h"
#include "storage/ThriftReader.h"
#include "surface/DataSurface.h"

/**
 * Kafka topic wrapping topic metadata store.
 */
namespace nebula {
namespace storage {
namespace kafka {

// Represents a reader of a kafka segment
class KafkaReader : public nebula::surface::RowCursor {
  static constexpr size_t SLICE_SIZE = 1024;

public:
  KafkaReader(nebula::meta::TableSpecPtr table,
              KafkaSegment segment,
              size_t timeoutMs = 3000)
    : nebula::surface::RowCursor(0),
      table_{ table },
      segment_{ std::move(segment) },
      timeoutMs_{ timeoutMs },
      row_{ SLICE_SIZE, true } {
    // initialize consumer and parser
    init();
  }

  virtual ~KafkaReader() {
    // unassign the partition
    consumer_->unassign();
    // consumer->unsubscribe();
    // TODO(cao): calling close will hanging forever.
    consumer_->close();
  }

public:
  // next row data of CsvRow
  virtual const nebula::surface::RowData& next() override;

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t) const override {
    throw NException("Kafka Reader does not support random access by row number");
  }

private:
  // load all messages in the repo
  void init();

  // next message
  std::unique_ptr<RdKafka::Message> message();

private:
  nebula::meta::TableSpecPtr table_;
  KafkaSegment segment_;
  size_t timeoutMs_;

  // queue of messages, when reader starts it will pumping messages into this queue
  nebula::memory::FlatRow row_;

  // kafka consumer and parser
  std::unique_ptr<RdKafka::TopicPartition> partition_;
  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
  std::unique_ptr<RowParser> parser_;
  int64_t max_;
  size_t errors_;
  std::unique_ptr<RdKafka::Message> msg_;
};

} // namespace kafka
} // namespace storage
} // namespace nebula