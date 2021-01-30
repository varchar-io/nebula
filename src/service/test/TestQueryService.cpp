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
#include "execution/core/NodeConnector.h"
#include "execution/core/ServerExecutor.h"
#include "service/base/NebulaService.h"
#include "service/server/QueryHandler.h"
#include "surface/DataSurface.h"
#include "surface/MockSurface.h"
#include "type/Serde.h"

namespace nebula {
namespace service {
namespace test {

using namespace nebula::api::dsl;
using nebula::common::Cursor;
using nebula::common::Evidence;
using nebula::execution::BlockManager;
using nebula::execution::core::NodeConnector;
using nebula::execution::core::ServerExecutor;
using nebula::service::base::ErrorCode;
using nebula::service::base::ServiceProperties;
using nebula::service::server::QueryHandler;
using nebula::surface::RowData;
using nebula::type::Schema;
using nebula::type::TypeSerializer;

} // namespace test
} // namespace service
} // namespace nebula
