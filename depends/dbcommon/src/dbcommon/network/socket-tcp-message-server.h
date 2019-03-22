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

#ifndef DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_SERVER_H_
#define DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_SERVER_H_

#include "dbcommon/network/socket-tcp.h"

#include <memory>
#include <string>
#include <unordered_map>

#include "dbcommon/network/socket-tcp-message-common.h"
#include "dbcommon/network/socket-tcp-message-serverhandler.h"
#include "dbcommon/thread/thread-base.h"

namespace dbcommon {

///////////////////////////////////////////////////////////////////////////////
// User should prepare and start one socket tcp server with specific handler
// factory to have this server run for different processing purposes.
//
// [SocketTcpMsgConnection]     [ThreadBase]    [AsyncCancelMark]
//           ^                       ^               ^
//           +----------+------------+---------------+
//                      | inherit
//             [SocketTcpMsgServer]--------->[SocketTcpMsgServerHandlerFactory]
//                      |             1:1                  ^
//                      |1:N                               | inherit
//                      |                         [Specific factories]
//        [ThreadBase]  |                                  |
//             ^        |                                  | create
//     inherit |        v                                  |
//          [SocketTcpMsgServerHandler]                    |
//                      ^                                  |
//                      | inherit                          |
//         [Specific TCP server handlers]<-----------------+
///////////////////////////////////////////////////////////////////////////////

class SocketTcpMsgServer : public SocketTcpMsgConnection,
                           public dbcommon::ThreadBase,
                           public AsyncCancelMark {
 public:
  SocketTcpMsgServer() : listenPort(0) {}
  virtual ~SocketTcpMsgServer() {
    stop();
    join();
  }

  void prepareListen(const std::string &localAddress, uint16_t localPort);
  void prepareFactory(std::unique_ptr<SocketTcpMsgServerHandlerFactory> f) {
    factory = std::move(f);
  }
  void run() override;

  void timeoutAccept(int *fd, uint32_t timeoutMs = SOCKET_ACCEPT_TIMEOUT_MS);

 protected:
  void setRequestCancel() override {}  // no used
  bool isCancelled() override { return this->threadExitConditionMet(); }

 protected:
  std::string listenAddr;  // listen address
  uint16_t listenPort;     // if this port is not set, server chooses port with
                           // the help from os.
  std::unique_ptr<SocketTcpMsgServerHandlerFactory> factory;

  std::unordered_map<uint64_t, std::unique_ptr<SocketTcpMsgServerHandler>>
      handlers;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_SERVER_H_
