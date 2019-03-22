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

#include "dbcommon/network/socket-tcp-message-server.h"

#include <poll.h>

namespace dbcommon {

void SocketTcpMsgServer::run() {
  // expect listen is done and factory is available
  assert(isConnected());
  assert(factory != nullptr);

  // main loop for accepting connections
  while (!threadExitConditionMet()) {
    int cfd = SOCKET_FD_INVALID;
    timeoutAccept(&cfd);
    if (cfd != SOCKET_FD_INVALID) {
      // call factory to build thread
      std::unique_ptr<SocketTcpMsgServerHandler> handler =
          std::move(factory->create(cfd));
      uint64_t hk = reinterpret_cast<uint64_t>(handler.get());
      handlers[hk] = std::move(handler);
      handlers[hk]->start();
    }
    // collect exited handler threads
    for (auto iter = handlers.begin(); iter != handlers.end();) {
      if (iter->second->threadExited()) {
        iter = handlers.erase(iter);  // free handler instance
      } else {
        iter++;
      }
    }
  }
  // actively close listening fd
  LOG_INFO("socket tcp server %p quits, close fd %d", this, this->getFd());
  this->close();

  // close and collect all running handler threads
  for (auto iter = handlers.begin(); iter != handlers.end();) {
    iter = handlers.erase(iter);  // free handler instance
  }
}

void SocketTcpMsgServer::prepareListen(const std::string &localAddress,
                                       uint16_t localPort) {
  // save address and port
  listenAddr = localAddress;
  listenPort = localPort;

  // start listening
  // step 1. resolve address
  struct addrinfo hints;
  ::memset(&hints, '\0', sizeof(struct addrinfo));
  hints.ai_family = AF_INET;        // Allow only IPv4
  hints.ai_socktype = SOCK_STREAM;  // TCP socket
  hints.ai_flags = AI_PASSIVE;      // For wildcard IP address
  hints.ai_protocol = 0;            // Any protocol

  struct addrinfo *addrs = nullptr;
  int ret = getaddrinfo(listenAddr.c_str(), std::to_string(listenPort).c_str(),
                        &hints, &addrs);
  if (ret != 0) {
    int en = errno;
    if (addrs != nullptr) {
      freeaddrinfo(addrs);
    }
    cleanupAndReportError("getaddrinfo", __FUNCTION__, en);
  }

  int sfd = SOCKET_FD_INVALID;
  for (struct addrinfo *rp = addrs; rp != nullptr; rp = rp->ai_next) {
    sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sfd == -1) {
      LOG_INFO(
          "socket() system call reported error (%d): %s, will try the next "
          "addr",
          sfd, strerror(errno));
      continue;
    }

    int optval = 1;
    int ret =
        ::setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (ret < 0) {
      ::close(sfd);
      LOG_INFO("setsocketopt system call reported error (%d): %s", ret,
               strerror(errno));
      continue;
    }

    if (bind(sfd, rp->ai_addr, rp->ai_addrlen) == 0) {
      break;  // successfully bind address
    } else {
      LOG_INFO(
          "bind() system call reported error (%d): %s, will try the next addr",
          sfd, strerror(errno));
    }

    ::close(sfd);
    sfd = SOCKET_FD_INVALID;
  }

  if (addrs != nullptr) {
    freeaddrinfo(addrs);
  }

  if (sfd == SOCKET_FD_INVALID) {
    LOG_ERROR(ERRCODE_SYSTEM_ERROR, "failed to find an address to bind");
  }

  ret = fcntl(sfd, F_SETFL, O_NONBLOCK);
  if (ret == -1) {
    ::close(sfd);
    LOG_ERROR(ERRCODE_SYSTEM_ERROR,
              "fcntl() setting nonblocking socket reported error (%d): %s", sfd,
              strerror(errno));
  }

  ret = ::listen(sfd, SOCKET_LISTEN_BACKLOG);
  if (ret < 0) {
    ::close(sfd);
    LOG_ERROR(ERRCODE_SYSTEM_ERROR,
              "listen() system call reported error (%d): %s", ret,
              strerror(errno));
  }

  this->setFd(sfd);
  LOG_INFO("socket tcp server %p listen on fd %d", this, this->getFd());
}

void SocketTcpMsgServer::timeoutAccept(int *fd, uint32_t timeoutMs) {
  *fd = SOCKET_FD_INVALID;  // initializ
  struct pollfd fds[1];
  memset(fds, 0, sizeof(struct pollfd) * 1);  // check only one fd
  fds[0].fd = this->getFd();
  fds[0].events = POLLIN | POLLERR | POLLHUP | POLLNVAL;
  int ret = poll(fds, 1, timeoutMs);
  if (ret == -1) {
    if (errno != EAGAIN && errno != EINTR) {
      cleanupAndReportError("poll", __FUNCTION__, errno);
    }
    return;  // skip this time for accepting
  } else if (ret == 0) {
    return;  // not ready for accepting when timed out
  } else {
    assert(ret == 1);  // as we check only one fd
    if (fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
      int ofd = this->fd;
      this->close();
      LOG_ERROR(ERRCODE_INTERCONNECT_ERROR,
                "socket server failure is encountered detected by poll() fd %d",
                ofd);
    }
    if (fds[0].revents & POLLIN) {
      // accept connection now
      // TODO(yi): accept to get address info
      int cfd = ::accept(getFd(), nullptr, 0);
      if (cfd == -1) {
        // if it is not a case we can retry, close and report error
        if (errno != EWOULDBLOCK && errno != EAGAIN && errno != EINTR) {
          cleanupAndReportError("recv", __FUNCTION__, errno);
        }
      } else if (cfd > 0) {
        *fd = cfd;
        return;  // accepted one connection
      } else {
        assert("accept() returned 0" && false);  // should never come here
      }
    } else {  // end of if (fds[0].revents & POLLIN)
      assert("unexpected poll revent" && false);
    }
  }
}

}  // namespace dbcommon
