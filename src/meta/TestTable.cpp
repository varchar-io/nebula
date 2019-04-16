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

#include "TestTable.h"

/**
 * Nebula test table used for integration test.
 * Used to turn on/off test hooks.
 */
namespace nebula {
namespace meta {

const std::string& TestTable::name() {
  static const std::string NAME = "nebula.test";
  return NAME;
}

const std::string& TestTable::schema() {
  static const std::string SCHEMA = "ROW<id:int, event:string, items:list<string>, flag:bool>";
  return SCHEMA;
}

const std::string& TestTable::trendsTableName() {
  static const std::string NAME = "trends.draft";
  return NAME;
}

const std::string& TestTable::trendsTableSchema() {
  static const std::string SCHEMA = "ROW<query:string, dt:string, count:int>";
  return SCHEMA;
}

} // namespace meta
} // namespace nebula