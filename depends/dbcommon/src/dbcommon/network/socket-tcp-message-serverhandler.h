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

#ifndef DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_SERVERHANDLER_H_
#define DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_SERVERHANDLER_H_

#include "dbcommon/network/socket-tcp-message-common.h"
#include "dbcommon/thread/thread-base.h"

namespace dbcommon {

////////////////////////////////////////////////////////////////////////////////
// This abstract class used to create socket handle thread, when one connection
// is accepted, this factory is responsible for building thread instance as
// server side handler.
////////////////////////////////////////////////////////////////////////////////
class SocketTcpMsgServerHandler;
class SocketTcpMsgServerHandlerFactory {
 public:
  virtual ~SocketTcpMsgServerHandlerFactory() {}
  virtual std::unique_ptr<SocketTcpMsgServerHandler> create(int fd) = 0;
};

// The abstract thread need to be implement by user
class SocketTcpMsgServerHandler : public SocketTcpMsgConnection,
                                  public dbcommon::ThreadBase {
 public:
  explicit SocketTcpMsgServerHandler(int fd) : SocketTcpMsgConnection(fd) {
    setFdNoDelay();
    setFdNonBlocking();
  }
  virtual ~SocketTcpMsgServerHandler() {
    stop();
    join();
  }

  virtual void run() = 0;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_SERVERHANDLER_H_
