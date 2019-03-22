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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_NET_CLIENT_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_NET_CLIENT_H_

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace dbcommon {
inline int connect(int fd, const struct sockaddr* addr, socklen_t len,
                   uint32_t retryCount, uint32_t retryIntervalMs) {
  int ret = -1;
  do {
    ret = ::connect(fd, addr, len);
    if (ret < 0) {
      usleep(1000 * retryIntervalMs);
      continue;
    } else {
      break;
    }
  } while (retryCount-- != 0);

  return ret;
}

}  // end namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_NET_CLIENT_H_
