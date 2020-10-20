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
#include "NebulaClient.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include "common/Folly.h"
#include "common/Ip.h"
#include "meta/NNode.h"
#include "service/base/NebulaService.h"
#include "service/node/NodeClient.h"

int main(int argc, char** argv) {
  const nebula::meta::NNode node{
    nebula::meta::NRole::NODE,
    "localhost",
    nebula::service::base::ServiceProperties::PORT
  };

  auto greeter = nebula::service::client::NebulaClient::make(node.toString());

  // ping the server
  nebula::service::ServiceInfo info;
  auto hi = nebula::common::Ip::hostInfo();

  info.set_ipv4(hi.ipv4);
  info.set_port(nebula::service::base::ServiceProperties::NPORT);
  auto p = greeter.ping(info);
  LOG(INFO) << "Echo received from nebula server: " << (p != nullptr);

  return 0;
}