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

#include "KafkaProvider.h"
#include "KafkaTopic.h"
#include "common/Errors.h"
#include "memory/FlatRow.h"
#include "meta/TableSpec.h"
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
              size_t timeoutMs = 5000)
    : nebula::surface::RowCursor(0),
      table_{ table },
      segment_{ std::move(segment) },
      timeoutMs_{ timeoutMs },
      row_{ SLICE_SIZE } {

    // reverse mapping of name -> id
    const auto& cmap = table_->serde.cmap;
    for (auto itr = cmap.begin(); itr != cmap.end(); ++itr) {
      fields_.emplace(itr->second, itr->first);
    }

    // load all desired messages through this consumer
    load(KafkaProvider::getConsumer(table_->location));
  }

  virtual ~KafkaReader() = default;

public:
  // next row data of CsvRow
  virtual const nebula::surface::RowData& next() override;

  virtual std::unique_ptr<nebula::surface::RowData> item(size_t) const override {
    throw NException("Kafka Reader does not support random access by row number");
  }

private:
  // load all messages in the repo
  void load(RdKafka::KafkaConsumer*);

  // populate current message into flat row with thrift and bianry protocol
  void populateThriftBinary(const RdKafka::Message&);

  void nullifyRow() noexcept {
    // write everything a null if encoutering an invalid message
    row_.reset();
    row_.write(nebula::meta::Table::TIME_COLUMN, 0l);
    for (auto itr = fields_.cbegin(); itr != fields_.cend(); ++itr) {
      row_.writeNull(itr->second);
    }
  }

private:
  nebula::meta::TableSpecPtr table_;
  KafkaSegment segment_;
  size_t timeoutMs_;

  // queue of messages, when reader starts it will pumping messages into this queue
  std::vector<std::unique_ptr<RdKafka::Message>> messages_;
  nebula::memory::FlatRow row_;

  // reverse the fields mapping from field ID -> name
  std::unordered_map<uint32_t, std::string> fields_;
};

} // namespace kafka
} // namespace storage
} // namespace nebula