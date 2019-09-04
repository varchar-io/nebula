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

namespace nebula {
namespace common {

enum TaskType : int8_t {
  INGESTION = 'I'
};

enum TaskState : int8_t {
  UNKNOWN = 'U',
  WAITING = 'W',
  PROCESSING = 'P',
  FAILED = 'F',
  SUCCEEDED = 'S'
};

class Signable {
public:
  virtual std::string signature() const = 0;
};

/**
 * A task represent a task to be executed on any node in the nebula.
 * Task is passed around in the queue system, make sure it is cheap to copy.
 */
class Task {
public:
  // a task is an instance owning the passed in spec pointer
  // release this spec when task exits
  Task(TaskType type, std::shared_ptr<Signable> spec) : type_{ type }, spec_{ spec } {}
  virtual ~Task() = default;

public:
  inline std::string signature() const {
    return spec_->signature();
  }

  inline TaskType type() const {
    return type_;
  }

  template <typename T>
  inline std::shared_ptr<T> spec() const {
    return std::static_pointer_cast<T>(spec_);
  }

private:
  TaskType type_;
  std::shared_ptr<Signable> spec_;
};

} // namespace common
} // namespace nebula