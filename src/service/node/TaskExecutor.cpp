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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "TaskExecutor.h"
#include "ingest/BlockExpire.h"
#include "ingest/IngestSpec.h"

DEFINE_uint32(TASK_QUEUE_SIZE, 5000, "task queue size for bounded queue");
DEFINE_int32(TASK_PRIORITY, -1, "0 is medium priority");

/**
 * Define node server that does the work as nebula server asks.
 */
namespace nebula {
namespace service {
namespace node {

using nebula::common::SingleCommandTask;
using nebula::common::Task;
using nebula::common::TaskState;
using nebula::common::TaskType;
using nebula::ingest::BlockExpire;
using nebula::ingest::IngestSpec;

TaskExecutor& TaskExecutor::singleton() {
  static TaskExecutor executor{ FLAGS_TASK_QUEUE_SIZE };
  return executor;
}

void TaskExecutor::process(std::function<void()> shutdown, folly::ThreadPoolExecutor& pool) {
  // process all items in sequence
  while (!queue_.isEmpty()) {
    Task* task = queue_.frontPtr();

    // mark this task state as processing
    auto sign = task->signature();
    state_[sign] = TaskState::PROCESSING;

    // fast command execution
    if (task->type() == TaskType::COMMAND) {
      auto c = task->spec<SingleCommandTask>();
      if (c->isShutdown()) {
        if (shutdown) {
          shutdown();
        }
      }
    }

    // process this task in async way
    // if the task failed, the task state may stay as "PROCESSING" forever.
    pool.addWithPriority(
      [this, s = std::move(sign), t = *task]() {
        setState(t.type(), s, process(t) ? TaskState::SUCCEEDED : TaskState::FAILED);
      },
      FLAGS_TASK_PRIORITY);

    // remove this task from queue
    queue_.popFront();
  }
}

// execute the task now in current service thread
TaskState TaskExecutor::execute(Task task) {
  // unique ID in the running system to avoid duplicate task
  const auto& sign = task.signature();
  TaskState found = search(sign);
  if (found != TaskState::NOTFOUND) {
    // TODO(cao) - if a task failed once, it may never get retried.
    // state stored with a timestamp and use a timer to scan and change states
    return found;
  }

  // execute the task and record its state
  setState(task.type(), sign, TaskState::PROCESSING);
  auto state = process(task) ? TaskState::SUCCEEDED : TaskState::FAILED;
  setState(task.type(), sign, state);

  return state;
}

// enqueue could be called by multiple session, need sync
TaskState TaskExecutor::enqueue(Task task) {
  std::lock_guard<std::mutex> guard(stateLock_);

  // unique ID in the running system to avoid duplicate task
  const auto& sign = task.signature();
  TaskState found = search(sign);
  if (found != TaskState::NOTFOUND) {
    VLOG(1) << "Task already in node, state=" << (char)found;
    return found;
  }

  // if not found, we queue this task, and set its state as waiting
  if (queue_.isFull()) {
    VLOG(1) << "Queue is full, can not enqueue task at this moment. Q-size: " << queue_.capacity();
    return TaskState::QUEUE;
  }

  // enqueue it for sure
  if (!queue_.write(std::move(task))) {
    LOG(ERROR) << "Failed to enqueue the task, unknown reason.";
    return TaskState::FAILED;
  }

  // save its state
  state_[sign] = TaskState::WAITING;
  return TaskState::WAITING;
}

bool TaskExecutor::process(const Task& task) {
  // handle all different type of tasks assigned by server
  LOG(INFO) << "processed task: " << task.signature();

  // handle ingestion task
  if (task.type() == TaskType::INGESTION) {
    std::shared_ptr<IngestSpec> is = task.spec<IngestSpec>();

    // process a new task - enroll its table if its first time
    return is->work();
  }

  if (task.type() == TaskType::EXPIRATION) {
    auto be = task.spec<BlockExpire>();

    if (be->work()) {
      for (const auto& spec : be->specs()) {
        auto task = Task::sign(spec.second, TaskType::INGESTION);
        if (state_.erase(task) > 0) {
          LOG(INFO) << "Spec is reset: " << spec.second;
        }
      }

      return true;
    }

    return false;
  }

  return false;
}

} // namespace node
} // namespace service
} // namespace nebula