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

#ifndef DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_CLIENT_H_
#define DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_CLIENT_H_

#include <string>

#include "dbcommon/network/socket-tcp-message-common.h"
#include "dbcommon/network/socket-tcp.h"

namespace dbcommon {

class SocketTcpMsgClient : public SocketTcpMsgConnection {
 public:
  SocketTcpMsgClient();
  virtual ~SocketTcpMsgClient();

  // connect to remote server
  void connect(const std::string &address, uint16_t port,
               int retry = SOCKET_CONN_RETRY);

 protected:
  int connectI(struct addrinfo *rp, bool *isValid, bool *connected, int *errNo);
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_CLIENT_H_
