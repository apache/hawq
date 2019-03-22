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

#include "dbcommon/network/socket-tcp-message-client.h"

#include <string.h>

#include <string>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/time-util.h"

namespace dbcommon {

SocketTcpMsgClient::SocketTcpMsgClient() {}

SocketTcpMsgClient::~SocketTcpMsgClient() {}

int SocketTcpMsgClient::connectI(struct addrinfo *rp, bool *isValid,
                                 bool *connected, int *errNo) {
  int tryCount = 0;
  int sfd = -1;

createSocketAgain:  // create socket again
  sfd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
  if (sfd == -1) {  // try another address
    *isValid = false;
    *errNo = errno;
    return 0;
  }

  int optval = 1;
  int ret =
      ::setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
  if (ret == -1) {
    *isValid = false;
    *errNo = errno;
    ::close(sfd);
    return 0;
  }

  *isValid = true;
  ret = ::connect(sfd, rp->ai_addr, rp->ai_addrlen);
  if (ret == 0) {  // success
    *connected = true;
    return sfd;
  } else {
    if (tryCount < SOCKET_CONN_RETRY) {
      LOG_WARNING("retry connection %d", tryCount);
      ::close(sfd);
      dbcommon::TimeUtil::usleep(SOCKET_CONN_INTERVAL_MS * 1000);  // interval
      tryCount++;
      goto createSocketAgain;
    } else {
      *errNo = errno;
      ::close(sfd);
      return -1;
    }
  }
}

void SocketTcpMsgClient::connect(const std::string &address, uint16_t port,
                                 int retry) {
  // step 1. resolve address
  struct addrinfo hints;
  ::memset(&hints, '\0', sizeof(struct addrinfo));
  hints.ai_family = AF_INET;        // Allow only IPv4
  hints.ai_socktype = SOCK_STREAM;  // TCP socket
  hints.ai_flags = AI_PASSIVE;      // For wildcard IP address
  hints.ai_protocol = 0;            // Any protocol

  struct addrinfo *addrs = nullptr;
  int ret = getaddrinfo(address.c_str(), std::to_string(port).c_str(), &hints,
                        &addrs);
  if (ret != 0) {
    int en = errno;
    if (addrs != nullptr) {
      freeaddrinfo(addrs);
    }
    cleanupAndReportError("getaddrinfo", __FUNCTION__, en);
  }

  for (struct addrinfo *rp = addrs; rp != NULL; rp = rp->ai_next) {
    int sfd = -1;

    int errNo = 0;
    bool isValid = false;
    bool connected = false;
    int ret = connectI(rp, &isValid, &connected, &errNo);

    if (ret == 0 && isValid == false) {
      continue;
    } else if (ret > 0) {
      setFd(ret);  // connected with one valid fd
      setFdNoDelay();
      setFdNonBlocking();
      break;
    } else {
      cleanupAndReportError("connectI", __FUNCTION__, errNo);
    }
  }  // end for (struct addrinfo *rp = addrs

  if (addrs != nullptr) {
    freeaddrinfo(addrs);  // we dont need addrs any longer
  }
}

}  // namespace dbcommon
