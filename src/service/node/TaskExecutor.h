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
#include <folly/executors/ThreadPoolExecutor.h>

#include "common/Hash.h"
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
  void process(std::function<void()>, folly::ThreadPoolExecutor&);

  // put the task in async queue
  nebula::common::TaskState enqueue(nebula::common::Task);

  // execute the task in current thread as in-sync
  nebula::common::TaskState execute(nebula::common::Task);

private:
  bool process(const nebula::common::Task&);

  inline void setState(nebula::common::TaskType type,
                       const std::string& sign,
                       nebula::common::TaskState state) noexcept {
    // expiration task can be issued repeatedly
    if (type == nebula::common::TaskType::EXPIRATION) {
      state_.erase(sign);
      return;
    }

    std::lock_guard<std::mutex> guard(stateLock_);
    state_[sign] = state;
  }

  nebula::common::TaskState search(const std::string& sign) const noexcept {
    // unique ID in the running system to avoid duplicate task
    auto itr = state_.find(sign);

    // found this task, it is already acked
    if (itr != state_.end()) {
      return itr->second;
    }

    return nebula::common::TaskState::NOTFOUND;
  }

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
  nebula::common::unordered_map<std::string, nebula::common::TaskState> state_;
  std::mutex stateLock_;
};

} // namespace node
} // namespace service
} // namespace nebula