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

#ifndef DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_COMMON_H_
#define DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_COMMON_H_

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <mutex>  // NOLINT
#include <string>

namespace dbcommon {

#define SOCKET_FD_INVALID -1

#define SOCKET_MESSAGE_TAG 0X1234
#define SOCKET_MESSAGE_ID_INVALID 0XFF

#define SOCKET_CONN_RETRY 10
#define SOCKET_CONN_INTERVAL_MS 1000
#define SOCKET_SEND_TIMEOUT_MS 250
#define SOCKET_RECV_TIMEOUT_MS 250
#define SOCKET_ACCEPT_TIMEOUT_MS 250

#define SOCKET_LISTEN_BACKLOG 10000

struct MessageHeader {
 public:
  MessageHeader()
      : protocolTag(SOCKET_MESSAGE_TAG),
        userMessageId(SOCKET_MESSAGE_ID_INVALID),
        reserved(0),
        userMessageLength(0) {}

  void set(uint8_t messageId, uint32_t messageLen) {
    userMessageId = messageId;
    userMessageLength = messageLen;
  }

 public:
  uint16_t protocolTag;   // constant tag
  uint8_t userMessageId;  // user defined message id
  uint8_t reserved;
  uint32_t userMessageLength;  // user message content length
};

class AsyncCancelMark {
 public:
  virtual ~AsyncCancelMark() {}
  virtual bool isCancelled() = 0;
  virtual void setRequestCancel() = 0;
};
/*
 *
  // sending related fields
  MessageHeader sendMessageHeader;
  const char *sendMessage;
  int32_t sendMessageLeftSize;

  // receiving related fields
  MessageHeader recvMessageHeader;
  std::string *recvMessage;
  uint8_t *recvMessageId;
  int32_t recvMessageLeftSize;
  bool recvIsMessageHeader;
 */
class SocketTcpMsgConnection {
 public:
  SocketTcpMsgConnection()
      : fd(SOCKET_FD_INVALID),
        sendMessage(nullptr),
        sendMessageLeftSize(0),
        recvMessage(nullptr),
        recvMessageId(nullptr),
        recvMessageLeftSize(0),
        recvIsMessageHeader(true) {}
  explicit SocketTcpMsgConnection(int fd)
      : fd(fd),
        sendMessage(nullptr),
        sendMessageLeftSize(0),
        recvMessage(nullptr),
        recvMessageId(nullptr),
        recvMessageLeftSize(0),
        recvIsMessageHeader(true) {}
  virtual ~SocketTcpMsgConnection() { this->close(); }

  // close connection
  void close() {
    std::lock_guard<std::mutex> guard(fdMutex);
    closeWithoutMutexLocked();
  }

  void closeWithoutMutexLocked() {
    if (fd > 0) {
      ::shutdown(fd, SHUT_RDWR);
      ::close(fd);
      fd = SOCKET_FD_INVALID;
    }
  }

  // check if the connection is created
  bool isConnected() {
    std::lock_guard<std::mutex> guard(fdMutex);
    return fd > 0;
  }
  // get fd value
  int getFd() {
    std::lock_guard<std::mutex> guard(fdMutex);
    return fd;
  }
  // set fd value
  void setFd(int fd) {
    std::lock_guard<std::mutex> guard(fdMutex);
    this->fd = fd;
  }

  void setFdNoDelay();
  void setFdRecvTimeout();
  void setFdNonBlocking();

 protected:
  // close fd and report error
  void cleanupAndReportError(const char *funcName, const char *callerName,
                             int errNo);
  void cleanupAndReportErrorWithoutMutexLocked(const char *funcName,
                                               const char *callerName,
                                               int errNo);

 public:  // for sending and receiving
  void prepareSend(uint8_t messageId, const char *message,
                   uint32_t messageLength);
  // timeout send, return left content size to send, 0 means finished
  int timeoutSend(uint32_t timeoutMs = SOCKET_SEND_TIMEOUT_MS);

  void prepareRecv(uint8_t *messageId, std::string *message);
  // timeout recv, return left content size to receive
  int timeoutRecv(uint32_t timeoutMs = SOCKET_SEND_TIMEOUT_MS);

  // sync send, if cancel mark instance is provided, user can cancel sending
  // through an async way
  void send(uint8_t messageId, const char *message, uint32_t messageLength,
            AsyncCancelMark *cancelMark);
  // sync recv, if cancel mark instance is provided, user can cancel receiving
  // through an async way
  void recv(uint8_t *messageId, std::string *message,
            AsyncCancelMark *cancelMark);

 protected:
  void processSending();
  void processRecving();

 protected:
  int fd;  // socket fd

  // sending related fields
  MessageHeader sendMessageHeader;
  const char *sendMessage;
  int32_t sendMessageLeftSize;

  // receiving related fields
  MessageHeader recvMessageHeader;
  std::string *recvMessage;
  uint8_t *recvMessageId;
  int32_t recvMessageLeftSize;
  bool recvIsMessageHeader;

  std::mutex fdMutex;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_NETWORK_SOCKET_TCP_MESSAGE_COMMON_H_
