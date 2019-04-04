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

#include "Table.h"
#include "TestTable.h"
#include "fmt/format.h"
#include "type/Serde.h"

/**
 * Nebula runtime / online meta data.
 */
namespace nebula {
namespace meta {

using nebula::type::TypeSerializer;

void Table::loadTable() {
  // TODO(cao) - really build up meta service to servce this function
  // throw NebulaException("Meta service not implemented yet.");

  // for integration testing only
  if (name_ == TestTable::name()) {
    schema_ = TypeSerializer::from(TestTable::schema());
  }
}
} // namespace meta
} // namespace nebula