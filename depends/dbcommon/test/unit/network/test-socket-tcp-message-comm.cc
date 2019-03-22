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

#include <memory>
#include <string>

#include "dbcommon/network/socket-tcp-message-client.h"
#include "dbcommon/network/socket-tcp-message-server.h"
#include "dbcommon/network/socket-tcp-message-serverhandler.h"

#include "gtest/gtest.h"

namespace dbcommon {

class SocketTcpMessageServerHandlerDummy : public SocketTcpMsgServerHandler,
                                           public AsyncCancelMark {
 public:
  explicit SocketTcpMessageServerHandlerDummy(int fd)
      : SocketTcpMsgServerHandler(fd), exceptionErrorCode(0) {}
  virtual ~SocketTcpMessageServerHandlerDummy() {}

  bool isCancelled() override { return threadExitConditionMet(); }
  void setRequestCancel() override {}

  void run() override {
    try {
      while (!threadExitConditionMet() && isConnected()) {
        uint8_t msgId = SOCKET_MESSAGE_ID_INVALID;
        std::string message;
        recv(&msgId, &message, this);
        send(msgId, message.c_str(), message.size(), this);
      }
    } catch (TransactionAbortException &e) {
      exceptionErrorCode = e.errCode();
      exceptionErrorMessage = e.what();
    }
  }

 public:
  int exceptionErrorCode;
  std::string exceptionErrorMessage;
};

class SocketTcpMessageServerHandlerFactoryDummy
    : public SocketTcpMsgServerHandlerFactory {
 public:
  SocketTcpMessageServerHandlerFactoryDummy() {}
  virtual ~SocketTcpMessageServerHandlerFactoryDummy() {}
  std::unique_ptr<SocketTcpMsgServerHandler> create(int fd) {
    std::unique_ptr<SocketTcpMsgServerHandler> handler(
        new SocketTcpMessageServerHandlerDummy(fd));
    return std::move(handler);
  }
};

class AsyncCancelMarkDummy : public AsyncCancelMark {
 public:
  virtual ~AsyncCancelMarkDummy() {}
  void setRequestCancel() {}  // no used
  bool isCancelled() { return false; }
};

TEST(TestSoketTcpMessageComm, Basic1) {
  // start one server
  dbcommon::SocketTcpMsgServer server;
  std::unique_ptr<dbcommon::SocketTcpMsgServerHandlerFactory> factory(
      new SocketTcpMessageServerHandlerFactoryDummy());
  server.prepareFactory(std::move(factory));
  server.prepareListen("127.0.0.1", 45000);
  server.start();

  // use client to communicate
  dbcommon::SocketTcpMsgClient client;
  client.connect("127.0.0.1", 45000);
  std::string sendmsg = "test";
  std::string recvmsg;
  uint8_t recvmsgid;
  AsyncCancelMarkDummy acm;
  client.send(2, sendmsg.c_str(), sendmsg.size(), &acm);
  client.recv(&recvmsgid, &recvmsg, &acm);
  std::cout << recvmsg << std::endl;

  client.close();

  // shutdown server
  server.stop();
}

}  // namespace dbcommon
