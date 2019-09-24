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

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <rdkafkacpp.h>

namespace nebula {
namespace storage {
namespace test {

static bool run = true;
static bool exit_eof = false;
static int eof_cnt = 0;
static int partition_cnt = 0;
static int verbosity = 3;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;
static void sigterm(int) {
  run = false;
}

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
      LOG(ERROR) << "\"STATS\": " << event.str();
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

class ExampleRebalanceCb : public RdKafka::RebalanceCb {
private:
  static void part_list_print(const std::vector<RdKafka::TopicPartition*>& partitions) {
    for (unsigned int i = 0; i < partitions.size(); i++)
      LOG(ERROR) << partitions[i]->topic() << "[" << partitions[i]->partition() << "], ";
  }

public:
  void rebalance_cb(RdKafka::KafkaConsumer* consumer,
                    RdKafka::ErrorCode err,
                    std::vector<RdKafka::TopicPartition*>& partitions) {
    LOG(ERROR) << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

    part_list_print(partitions);

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      consumer->assign(partitions);
      partition_cnt = (int)partitions.size();
    } else {
      consumer->unassign();
      partition_cnt = 0;
    }
    eof_cnt = 0;
  }
};

void msg_consume(RdKafka::Message* message, void*) {
  switch (message->err()) {
  case RdKafka::ERR__TIMED_OUT:
    break;

  case RdKafka::ERR_NO_ERROR:
    /* Real message */
    msg_cnt++;
    msg_bytes += message->len();
    if (verbosity >= 3)
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
      LOG(INFO) << "Key: " << *message->key() << std::endl;
    }
    if (verbosity >= 1) {
      LOG(INFO) << static_cast<int>(message->len()) << static_cast<const char*>(message->payload());
    }
    break;

  case RdKafka::ERR__PARTITION_EOF:
    /* Last message */
    if (exit_eof && ++eof_cnt == partition_cnt) {
      LOG(ERROR) << "%% EOF reached for all " << partition_cnt << " partition(s)";
      run = false;
    }
    break;

  case RdKafka::ERR__UNKNOWN_TOPIC:
  case RdKafka::ERR__UNKNOWN_PARTITION:
    LOG(ERROR) << "Consume failed: " << message->errstr();
    run = false;
    break;

  default:
    /* Errors */
    LOG(ERROR) << "Consume failed: " << message->errstr();
    run = false;
  }
}

TEST(KafkaTest, TestLibKafkaConsumer) {
  // "Usage: %s -g <group-id> [options] topic1 topic2..\n"
  // "\n"
  // "librdkafka version %s (0x%08x)\n"
  // "\n"
  // " Options:\n"
  // "  -g <group-id>   Consumer group id\n"
  // "  -b <brokers>    Broker address (localhost:9092)\n"
  // "  -z <codec>      Enable compression:\n"
  // "                  none|gzip|snappy\n"
  // "  -e              Exit consumer when last message\n"
  // "                  in partition has been received.\n"
  // "  -d [facs..]     Enable debugging contexts:\n"
  // "                  %s\n"
  // "  -M <intervalms> Enable statistics\n"
  // "  -X <prop=name>  Set arbitrary librdkafka "
  // "configuration property\n"
  // "                  Properties prefixed with \"topic.\" "
  // "will be set on topic object.\n"
  // "                  Use '-X list' to see the full list\n"
  // "                  of supported properties.\n"
  // "  -q              Quiet / Decrease verbosity\n"
  // "  -v              Increase verbosity\n"
  // "\n"
  // "\n",
  // argv[0],
  // RdKafka::version_str().c_str(), RdKafka::version(),
  // RdKafka::get_debug_contexts().c_str());
  std::string brokers = "<BROKER>";
  std::string errstr;
  std::vector<std::string> topics{ "<TOPIC>" };

  /*
   * Create configuration objects
   */
  RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  ExampleRebalanceCb ex_rebalance_cb;
  conf->set("rebalance_cb", &ex_rebalance_cb, errstr);
  conf->set("enable.partition.eof", "true", errstr);

  // set group ID = 1
  if (conf->set("group.id", "1", errstr) != RdKafka::Conf::CONF_OK) {
    LOG(ERROR) << errstr;
    exit(1);
  }

  // set compression
  if (conf->set("compression.codec", "gzip", errstr) != RdKafka::Conf::CONF_OK) {
    LOG(ERROR) << errstr;
    exit(1);
  }

  // exit when reaching end of stream
  exit_eof = true;

  // set statistics printing interval at every 5 seconds
  if (conf->set("statistics.interval.ms", "5000", errstr) != RdKafka::Conf::CONF_OK) {
    LOG(ERROR) << errstr;
    exit(1);
  }

  /*
   * Set configuration properties
   */
  conf->set("metadata.broker.list", brokers, errstr);

  // run example event cb
  ExampleEventCb ex_event_cb;
  conf->set("event_cb", &ex_event_cb, errstr);

  // dump all configs
  for (auto pass = 0; pass < 2; pass++) {
    std::list<std::string>* dump;
    if (pass == 0) {
      dump = conf->dump();
      LOG(INFO) << "# Global config";
    } else {
      dump = tconf->dump();
      LOG(INFO) << "# Topic config";
    }

    for (std::list<std::string>::iterator it = dump->begin(); it != dump->end();) {
      LOG(INFO) << *it++ << " = " << *it++;
    }
  }

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
  while (run && count++ < 10) {
    RdKafka::Message* msg = consumer->consume(1000);
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

} // namespace test
} // namespace storage
} // namespace nebula