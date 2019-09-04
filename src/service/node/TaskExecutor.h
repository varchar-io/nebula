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

#include <folly/ProducerConsumerQueue.h>
#include <unordered_set>
#include "common/Task.h"

/**
 * Define a task executor which maintains queues for task status query.
 */
namespace nebula {
namespace service {
namespace node {

class TaskExecutor {
private:
  TaskExecutor(uint32_t size) : queue_{ size } {};
  TaskExecutor(TaskExecutor&) = delete;
  TaskExecutor(TaskExecutor&&) = delete;

public:
  virtual ~TaskExecutor() = default;

public:
  static TaskExecutor& singleton();

public:
  void process();

  nebula::common::TaskState enqueue(const nebula::common::Task&);

private:
  bool process(const nebula::common::Task&);

private:
  // TODO(cao): this is a quick shortcut implementation
  // It uses two maps for fast lookup:
  //    one is for tasks waiting for process
  //    the other is for tasks already processed
  // it has thread-safe issue and may require redesign soon.
  // the task in process may still in pending_
  // use folly::ProducerConsumerQueue<folly::fbstring> queue{size}; for processing
  // use std::unordered_map<signature, state> for query (not cleared up)
  folly::ProducerConsumerQueue<nebula::common::Task> queue_;
  std::unordered_map<std::string, nebula::common::TaskState> state_;
};

} // namespace node
} // namespace service
} // namespace nebula