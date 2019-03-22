/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "dbcommon/network/socket-tcp.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <cassert>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/time-util.h"

namespace dbcommon {

#define SOCKET_TCP_UTIL_RETRY_TIMES 10
#define SOCKET_TCP_UTIL_RETRY_INTERVAL_MS 200

std::string SocketTcpUtil::getIPByHostName(const std::string &hostname,
                                           uint16_t port) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET;        // Allow only IPv4
  hints.ai_socktype = SOCK_STREAM;  // TCP socket
  hints.ai_flags = AI_PASSIVE;      // For wildcard IP address
  hints.ai_protocol = 0;            // Any protocol

  struct addrinfo *addrs = nullptr;
  int ret = -1;
  for (int i = 0; i < SOCKET_TCP_UTIL_RETRY_TIMES; ++i) {
    ret = getaddrinfo(hostname.c_str(), std::to_string(port).c_str(), &hints,
                      &addrs);
    if (ret == 0) {
      break;  // got result already
    } else if (ret == EAI_AGAIN) {
      LOG_WARNING(
          "try again later to call getaddrinfo() as EAI_AGAIN is "
          "encountered");
      dbcommon::TimeUtil::usleep(SOCKET_TCP_UTIL_RETRY_INTERVAL_MS * 1000);
    } else {
      break;  // not a case to retry
    }
  }

  if (ret != 0) {
    if (addrs != nullptr) {
      freeaddrinfo(addrs);
    }
    LOG_WARNING(
        "failed to resolve host %s:%d to address while calling "
        "getaddrinfo()",
        hostname.c_str(), port);
    return "";
  }

  if (addrs == nullptr) {
    LOG_WARNING(
        "failed to get IPv4 address from host %s:%d after calling "
        "getaddrinfo()",
        hostname.c_str(), port);
    return "";
  }

  for (struct addrinfo *rp = addrs; rp != NULL; rp = rp->ai_next) {
    struct sockaddr_in *sin = (struct sockaddr_in *)(rp->ai_addr);
    char ip[INET_ADDRSTRLEN];
    const char *c = inet_ntop(AF_INET, &(sin->sin_addr), ip, sizeof(ip));
    freeaddrinfo(addrs);
    if (c == nullptr) {
      LOG_WARNING("failed to call inet_ntop() to convert ip address");
      return "";
    } else {
      LOG_DEBUG("resolved host %s to address %s", hostname.c_str(), c);
      return std::string(c);
    }
  }

  if (addrs != nullptr) {
    freeaddrinfo(addrs);
  }
  assert(false);  // should never come here
  return "";
}

}  // namespace dbcommon
