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

#include <arpa/inet.h>
#include <netdb.h>
#include <string>
#include <unistd.h>

#include "common/Errors.h"

namespace nebula {
namespace common {

// IP address utility
struct HostInfo {
  std::string host;
  std::string ipv4;
};

class Ip final {
  static constexpr auto LOCALHOST = "localhost";

public:
  static HostInfo hostInfo(bool localhost = true) {
    // use hints to control API returns
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags |= AI_PASSIVE;

    // get host name if not using localhost
    char host[1024];
    if (!localhost) {
      if (gethostname(host, sizeof(host)) != 0) {
        throw NException("Can't obtain current host name");
      }
    } else {
      std::strcpy(host, LOCALHOST);
    }

    // try to get the IP address
    struct addrinfo* res;
    auto errcode = getaddrinfo(host, "0", &hints, &res);
    if (errcode != 0) {
      throw NException("Can't obtain ip address.");
    }

    // parse the res
    char addrstr[100];
    constexpr auto size = sizeof(addrstr);
    while (res) {
      inet_ntop(res->ai_family, res->ai_addr->sa_data, addrstr, size);

      void* ptr = nullptr;
      switch (res->ai_family) {
      case AF_INET:
        ptr = &((struct sockaddr_in*)res->ai_addr)->sin_addr;
        break;
      case AF_INET6:
        ptr = &((struct sockaddr_in6*)res->ai_addr)->sin6_addr;
        break;
      }
      inet_ntop(res->ai_family, ptr, addrstr, size);

      // get the first one ipv4
      if (res->ai_family != PF_INET6) {
        break;
      }
      res = res->ai_next;
    }

    return HostInfo{ host, addrstr };
  }
};

} // namespace common
} // namespace nebula