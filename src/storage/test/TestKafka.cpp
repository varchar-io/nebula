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

  #include <fmt/format.h>
  #include <glog/logging.h>
  #include <gtest/gtest.h>
  #include <rdkafkacpp.h>
  #include <thrift/protocol/TBinaryProtocol.h>
  #include <thrift/protocol/TCompactProtocol.h>
  #include <thrift/transport/TBufferTransports.h>

  #include "common/Evidence.h"
  #include "meta/TableSpec.h"
  #include "storage/kafka/KafkaReader.h"

  namespace nebula {
  namespace storage {
  namespace test {

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::protocol::TType;
using apache::thrift::transport::TMemoryBuffer;

static bool run = true;
static bool exit_eof = false;
static int eof_cnt = 0;
static int verbosity = 3;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;
static void sigterm(int) {
  run = false;
}

static const auto BROKERS = "<brokers>";
static const auto TOPIC = "<topic>";

class ExampleEventCb : public RdKafka::EventCb {
public:
  virtual void event_cb(RdKafka::Event& event) override {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      if (event.fatal()) {
        LOG(FATAL) << "event error";
        run = false;
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

static void part_list_print(RdKafka::KafkaConsumer* consumer,
                            RdKafka::TopicPartition* p) {
  // query low and high offset of this partition 1820139
  int64_t lowOffset = -1;
  int64_t highOffset = -1;
  auto e = consumer->query_watermark_offsets(
    p->topic(), p->partition(), &lowOffset, &highOffset, 5000);
  if (e != RdKafka::ERR_NO_ERROR) {
    LOG(ERROR) << "Error: " << e;
  }
  LOG(INFO) << "Low offset: " << lowOffset << ", High offset:" << highOffset;
  LOG(ERROR) << p->topic() << "[" << p->partition() << "]: " << p->offset();
};

class ExampleRebalanceCb : public RdKafka::RebalanceCb {
public:
  void rebalance_cb(RdKafka::KafkaConsumer* consumer,
                    RdKafka::ErrorCode err,
                    std::vector<RdKafka::TopicPartition*>& partitions) {
    LOG(ERROR) << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      consumer->assign({ partitions.at(0) });
    } else {
      consumer->unassign();
    }

    eof_cnt = 0;
  }
};

void read(uint8_t* data, size_t size) {
  auto buffer = std::make_shared<TMemoryBuffer>(data, size);
  TBinaryProtocol proto(buffer);
  std::string name = "demo";

  // TBinaryProtocol starts with first field
  // auto length = proto.readStructBegin(name);

  // read all fields
  while (true) {
    TType type = apache::thrift::protocol::T_STOP;
    int16_t id = 0;

    proto.readFieldBegin(name, type, id);

    // no more fields to read
    if (id == 0) {
      break;
    }

#define TYPE_EXTRACT(T, CT, M)                  \
  case TType::T: {                              \
    CT v;                                       \
    proto.M(v);                                 \
    LOG(INFO) << id << "(" << #CT << "):" << v; \
    break;                                      \
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

    proto.readFieldEnd();
  }
}

bool msg_consume(RdKafka::Message* message, void*) {
  switch (message->err()) {
  case RdKafka::ERR__TIMED_OUT:
    return false;

  case RdKafka::ERR_NO_ERROR: { /* Real message */
    msg_cnt++;
    msg_bytes += message->len();
    LOG(ERROR) << "Read msg at offset " << message->offset();
    RdKafka::MessageTimestamp ts;
    ts = message->timestamp();
    if (verbosity >= 2 && ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
      std::string tsname = "?";
      if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
        tsname = "create time";
      else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
        tsname = "log append time";
      LOG(INFO) << "Timestamp: " << tsname << " " << ts.timestamp;
    }

    if (verbosity >= 2 && message->key()) {
      LOG(INFO) << "Key: " << *message->key();
    }

    // parse the payload using thrift deserialization
    LOG(INFO) << static_cast<int>(message->len()) << static_cast<const char*>(message->payload());
    auto data = static_cast<uint8_t*>(message->payload());
    auto size = message->len();
    read(data, size);

    return true;
  }

  case RdKafka::ERR__PARTITION_EOF:
  case RdKafka::ERR__UNKNOWN_TOPIC:
  case RdKafka::ERR__UNKNOWN_PARTITION: {
    LOG(ERROR) << "Consume failed: " << message->errstr();
    run = false;
    return false;
  }

  default: {
    /* Errors */
    LOG(ERROR) << "Consume failed: " << message->errstr();
    run = false;
    return false;
  }
  }
}

TEST(KafkaTest, DISABLED_TestLibKafkaConsumer) {
  std::string brokers = BROKERS;
  std::string errstr;
  std::vector<std::string> topics{ TOPIC };

  /*
   * Create configuration objects
   */
  RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  ExampleRebalanceCb ex_rebalance_cb;
  conf->set("rebalance_cb", &ex_rebalance_cb, errstr);
  // conf->set("enable.partition.eof", "true", errstr);

  // set group ID = 1
  if (conf->set("group.id", "1", errstr) != RdKafka::Conf::CONF_OK) {
    LOG(ERROR) << errstr;
    exit(1);
  }

  // exit when reaching end of stream
  exit_eof = true;

  /*
   * Set configuration properties
   */
  conf->set("metadata.broker.list", brokers, errstr);

  // run example event cb
  ExampleEventCb ex_event_cb;
  conf->set("event_cb", &ex_event_cb, errstr);

  // no auto offset store
  conf->set("enable.auto.offset.store", "false", errstr);

  conf->set("default_topic_conf", tconf, errstr);
  delete tconf;

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  /*
   * Create consumer using accumulated global configuration.
   */
  RdKafka::KafkaConsumer* consumer = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!consumer) {
    LOG(INFO) << "Failed to create consumer: " << errstr;
    exit(1);
  }

  delete conf;

  LOG(INFO) << "% Created consumer " << consumer->name();

  // test out partition info
  auto ts = nebula::common::Evidence::time("2019-09-27 11:00:00", "%Y-%m-%d %H:%M:%S");
  RdKafka::TopicPartition* p = RdKafka::TopicPartition::create(TOPIC, 0, ts * 1000);
  std::vector<RdKafka::TopicPartition*> offsettoppars{ p };
  consumer->assign(offsettoppars);
  consumer->offsetsForTimes(offsettoppars, 1000);

  part_list_print(consumer, p);
  LOG(ERROR) << p->topic() << "[" << p->partition() << "]: " << p->offset();

  /*
   * Subscribe to topics
   */
  RdKafka::ErrorCode err = consumer->subscribe(topics);
  if (err) {
    LOG(ERROR) << "Failed to subscribe to " << topics.size() << " topics: " << RdKafka::err2str(err);
    exit(1);
  }

  /*
   * Consume messages
   */
  auto count = 0;
  while (run && count++ < 3) {
    LOG(INFO) << "READ a message";
    RdKafka::Message* msg = consumer->consume(5000);
    msg_consume(msg, NULL);
    delete msg;
  }

  /*
   * Stop consumer
   */
  consumer->close();
  delete consumer;

  LOG(INFO) << "% Consumed " << msg_cnt << " messages (" << msg_bytes << " bytes)";

  /*
   * Wait for RdKafka to decommission.
   * This is not strictly needed (with check outq_len() above), but
   * allows RdKafka to clean up all its resources before the application
   * exits so that memory profilers such as valgrind wont complain about
   * memory leaks.
   */
  RdKafka::wait_destroyed(5000);
}

TEST(KafkaTest, DISABLED_TestKafkaTopic) {
  nebula::meta::KafkaSerde serde;
  nebula::storage::kafka::KafkaTopic topic(BROKERS, TOPIC, serde);

  // 10 hours ago
  auto tenHr = nebula::common::Evidence::unix_timestamp() - 3600 * 10;
  auto segments1 = topic.segmentsByTimestamp(tenHr * 1000, 50000);
  for (auto& s : segments1) {
    if (s.partition == 0) {
      LOG(INFO) << "" << s.id();
    }
  }
}

TEST(KafkaTest, DISABLED_TestKafkaReader) {
  nebula::meta::KafkaSerde serde;
  serde.protocol = "binary";
  serde.cmap = { { "id", 1 }, { "referer", 3 }, { "country", 6 } };
  auto topic = std::make_unique<nebula::storage::kafka::KafkaTopic>(BROKERS, TOPIC, serde);

  // 10 hours ago
  auto tenHr = nebula::common::Evidence::unix_timestamp() - 3600 * 10;
  auto segments = topic->segmentsByTimestamp(tenHr * 1000, 5000);
  N_ENSURE(segments.size() > 0, "more than 0 segments");
  LOG(INFO) << "Generated " << segments.size() << " segments. Pick first one to read";

  nebula::meta::ColumnProps cp;
  nebula::meta::TimeSpec ts;
  nebula::meta::AccessSpec as;
  nebula::meta::BucketInfo bi = nebula::meta::BucketInfo::empty();
  std::unordered_map<std::string, std::string> settings;
  ts.type = nebula::meta::TimeType::CURRENT;

  auto table = std::make_shared<nebula::meta::TableSpec>(
    TOPIC, 1000, 100, "ROW<id:string, referer:string, country:string>",
    nebula::meta::DataSource::KAFKA, "Roll", BROKERS, "",
    "thrift", std::move(serde), std::move(cp), std::move(ts),
    std::move(as), std::move(bi), std::move(settings));

  const auto& seg = segments.front();
  nebula::storage::kafka::KafkaReader reader(table, seg);
  EXPECT_EQ(reader.size(), seg.size);
  std::vector<int64_t> times;
  times.reserve(seg.size);
  while (reader.hasNext()) {
    auto& row = reader.next();
    times.push_back(row.readLong(nebula::meta::Table::TIME_COLUMN));
  }

  // read it again and check every time stamp is the same
  // to ensure each spec is reproduciable
  nebula::storage::kafka::KafkaReader reader2(table, seg);
  EXPECT_EQ(reader2.size(), seg.size);
  auto i = 0;
  while (reader2.hasNext()) {
    auto& row = reader2.next();
    EXPECT_EQ(times.at(i++),
              row.readLong(nebula::meta::Table::TIME_COLUMN));
  }
}

TEST(KafkaTest, TestKafkaSegmentSerde) {
  nebula::storage::kafka::KafkaSegment seg{ 2, 5499999, 3200010 };
  auto id = seg.id();
  LOG(INFO) << "Segment ID=" << id;
  auto seg2 = nebula::storage::kafka::KafkaSegment::from(id);
  auto id2 = seg2.id();
  LOG(INFO) << "Segment2 ID=" << id2;
  EXPECT_EQ(id, id2);
  // EXPECT_EQ(seg, seg2);
}

TEST(KafkaTest, DISABLED_TestSimpleNestedSchema) {
  nebula::meta::KafkaSerde serde;
  serde.protocol = "binary";
  serde.cmap = { { "_time_", 1 },
                 { "userId", 3001 },
                 { "magicType", 3003 },
                 { "statusCode", 4002 },
                 { "count", 4001 },
                 { "error", 4003 } };

  auto topic = std::make_unique<nebula::storage::kafka::KafkaTopic>("<brokers>", "<topic>", serde);

  // 10 hours ago
  auto tenHr = nebula::common::Evidence::unix_timestamp() - 3600 * 2;
  auto segments = topic->segmentsByTimestamp(tenHr * 1000, 5000);
  N_ENSURE(segments.size() > 0, "more than 0 segments");
  LOG(INFO) << "Generated " << segments.size() << " segments. Pick first one to read";

  nebula::meta::ColumnProps cp;
  nebula::meta::TimeSpec ts;
  nebula::meta::AccessSpec as;
  nebula::meta::BucketInfo bi = nebula::meta::BucketInfo::empty();
  std::unordered_map<std::string, std::string> settings;
  ts.type = nebula::meta::TimeType::CURRENT;

  auto table = std::make_shared<nebula::meta::TableSpec>(
    "<topic>", 1000, 100, "ROW<userId:long, magicType:short, statusCode:byte, count:int, error:string>",
    nebula::meta::DataSource::KAFKA, "Roll", "<brokers>", "",
    "thrift", std::move(serde), std::move(cp), std::move(ts),
    std::move(as), std::move(bi), std::move(settings));

  auto count = 0;
  for (auto& seg : segments) {
    if (count++ > 1) {
      break;
    }

    nebula::storage::kafka::KafkaReader reader(table, seg);
    LOG(INFO) << "Message count=" << reader.size() << " for segment=" << seg.id();

#define NULL_OR_VALUE(COL, F)               \
  if (row.isNull(COL)) {                    \
    LOG(INFO) << COL << ": NULL";           \
  } else {                                  \
    LOG(INFO) << COL << ": " << row.F(COL); \
  }

    while (reader.hasNext()) {
      auto& row = reader.next();
      NULL_OR_VALUE(nebula::meta::Table::TIME_COLUMN, readLong)
      NULL_OR_VALUE("userId", readLong)
      NULL_OR_VALUE("magicType", readShort)
      NULL_OR_VALUE("statusCode", readByte)
      NULL_OR_VALUE("count", readInt)
      NULL_OR_VALUE("error", readString)
    }
  }

#undef NULL_OR_VALUE
}

TEST(KafkaTest, DISABLED_TestFetchConfig) {
  nebula::meta::KafkaSerde serde;
  serde.retention = 90000;
  serde.size = 60000;
  auto topic = std::make_unique<nebula::storage::kafka::KafkaTopic>("<broker>>", "<topic>", serde);

  // 1 hours ago
  auto oneHr = nebula::common::Evidence::unix_timestamp() - 3600 * 1;
  auto segments = topic->segmentsByTimestamp(oneHr * 1000, 5000);
  N_ENSURE(segments.size() > 0, "more than 0 segments");
  LOG(INFO) << "Generated " << segments.size() << " segments. Pick first one to read";
}

} // namespace test
} // namespace storage
} // namespace nebula