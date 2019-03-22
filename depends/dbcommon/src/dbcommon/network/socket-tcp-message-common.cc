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

#include "dbcommon/network/socket-tcp-message-common.h"

#include <poll.h>

#include <cassert>

#include "dbcommon/log/logger.h"

namespace dbcommon {

void SocketTcpMsgConnection::setFdNoDelay() {
  int flag = 1;
  std::lock_guard<std::mutex> guard(fdMutex);
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
    cleanupAndReportErrorWithoutMutexLocked("setsockopt", __FUNCTION__, errno);
  }
}

void SocketTcpMsgConnection::setFdRecvTimeout() {
  struct timeval timeout;
  timeout.tv_sec = 0;
  timeout.tv_usec = SOCKET_RECV_TIMEOUT_MS * 1000;
  std::lock_guard<std::mutex> guard(fdMutex);
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&timeout,
                   sizeof(timeout)) < 0) {
    cleanupAndReportErrorWithoutMutexLocked("setsockopt", __FUNCTION__, errno);
  }
}

void SocketTcpMsgConnection::setFdNonBlocking() {
  std::lock_guard<std::mutex> guard(fdMutex);
  // set nonblocking
  int ret2 = ::fcntl(fd, F_SETFL, O_NONBLOCK);
  if (ret2 == -1) {
    cleanupAndReportErrorWithoutMutexLocked("fcntl", __FUNCTION__, errno);
  }
}

void SocketTcpMsgConnection::cleanupAndReportError(const char *funcName,
                                                   const char *callerName,
                                                   int errNo) {
  this->close();
  LOG_ERROR(ERRCODE_SYSTEM_ERROR,
            "socket connection failure is encountered while calling %s() "
            "in %s(), errno %d",
            funcName, callerName, errNo);
}

void SocketTcpMsgConnection::cleanupAndReportErrorWithoutMutexLocked(
    const char *funcName, const char *callerName, int errNo) {
  this->closeWithoutMutexLocked();
  LOG_ERROR(ERRCODE_SYSTEM_ERROR,
            "socket connection failure is encountered while calling %s() "
            "in %s(), errno %d",
            funcName, callerName, errNo);
}

void SocketTcpMsgConnection::prepareSend(uint8_t messageId, const char *message,
                                         uint32_t messageLength) {
  if (sendMessageLeftSize > 0) {
    LOG_WARNING("found not completed message for sending");
  }
  sendMessageHeader.userMessageId = messageId;
  sendMessageHeader.userMessageLength = messageLength;
  sendMessage = message;
  // reset counter of left bytes to send
  sendMessageLeftSize = sizeof(MessageHeader) + messageLength;
}

void SocketTcpMsgConnection::processSending() {
  int offset = sendMessageLeftSize - sendMessageHeader.userMessageLength;
  // send header as the first step
  if (offset > 0) {
    std::lock_guard<std::mutex> guard(fdMutex);
    int ret = ::send(fd,
                     (reinterpret_cast<char *>(&sendMessageHeader)) +
                         sizeof(MessageHeader) - offset,
                     offset, 0);
    if (ret >= 0) {
      sendMessageLeftSize -= ret;
    } else {
      if (errno != EINTR && errno != EWOULDBLOCK) {
        cleanupAndReportErrorWithoutMutexLocked("send", __FUNCTION__, errno);
      }
    }
  }

  // send message content
  offset = sendMessageLeftSize - sendMessageHeader.userMessageLength;
  if (offset <= 0 && sendMessageLeftSize > 0) {
    std::lock_guard<std::mutex> guard(fdMutex);
    int ret = ::send(fd, const_cast<char *>(sendMessage) - offset,
                     sendMessageLeftSize, 0);
    if (ret >= 0) {
      sendMessageLeftSize -= ret;
    } else {
      if (errno != EINTR && errno != EWOULDBLOCK) {
        cleanupAndReportErrorWithoutMutexLocked("send", __FUNCTION__, errno);
      }
    }
  }
}

// timeout send, return left content size to send
int32_t SocketTcpMsgConnection::timeoutSend(uint32_t timeoutMs) {
  assert(sendMessageHeader.userMessageLength == 0 || sendMessage != nullptr);
  assert(sendMessageLeftSize != 0);  // we only expect this is called when
                                     // something should be sent
  struct pollfd fds[1];
  memset(fds, 0, sizeof(struct pollfd) * 1);  // check only one fd
  fds[0].fd = getFd();
  fds[0].events = POLLIN | POLLOUT | POLLERR | POLLHUP | POLLNVAL;
  int result = poll(fds, 1, timeoutMs);
  if (result == -1) {
    if (errno != EAGAIN && errno != EINTR) {
      cleanupAndReportError("poll", __FUNCTION__, errno);
    }
    return sendMessageLeftSize;  // skip this time for sending
  } else if (result == 0) {
    return sendMessageLeftSize;  // not ready for sending when timed out
  } else {
    assert(result == 1);  // as we check only one fd
    if (fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
      int ofd = getFd();
      this->close();
      LOG_ERROR(
          ERRCODE_INTERCONNECT_ERROR,
          "socket connection failure is encountered detected by poll() fd %d",
          ofd);
    }
    if (fds[0].revents & POLLOUT) {
      // send content now
      processSending();
    } else if (fds[0].revents & POLLIN) {
      // should receive only 0 if the peer has been closed, we dont expect
      // actual content received as we dont expect the peer to send anything
      char buf[1];
      int receiveCount = 0;
      {
        std::lock_guard<std::mutex> guard(fdMutex);
        receiveCount = ::recv(fd, buf, sizeof(buf), 0);
      }
      if (receiveCount == 0) {
        // peer gracefully closed connection
        int ofd = getFd();
        this->close();
        LOG_ERROR(ERRCODE_INTERCONNECT_ERROR,
                  "socket connection found peer closed gracefully, close fd %d",
                  ofd);
      } else if (receiveCount > 0) {
        // received unexpected content, close and report error
        int ofd = getFd();
        this->close();
        LOG_ERROR(ERRCODE_INTERCONNECT_ERROR,
                  "socket connection got unexpected content, close fd %d", ofd);
      } else {
        // if it is not a case we can retry, close and report error
        if (errno != EWOULDBLOCK && errno != EAGAIN && errno != EINTR) {
          cleanupAndReportError("recv", __FUNCTION__, errno);
        }
        // else we accept this situation and should try again later
      }
    } else {  // end of if (fds[0].revents & POLLIN)
      assert("unexpected poll revent" && false);
    }
  }
  return sendMessageLeftSize;
}

void SocketTcpMsgConnection::prepareRecv(uint8_t *messageId,
                                         std::string *message) {
  if (recvMessageLeftSize != 0) {
    LOG_WARNING("found not completed message for receiving");
  }
  recvMessageId = messageId;
  recvMessage = message;
  recvMessageLeftSize = sizeof(MessageHeader);
  recvIsMessageHeader = true;
}

int SocketTcpMsgConnection::timeoutRecv(uint32_t timeoutMs) {
  assert(recvMessageId != nullptr && recvMessage != nullptr);
  assert(recvMessageLeftSize > 0);  // we only expect this is called when
                                    // something should be received
  struct pollfd fds[1];
  memset(fds, 0, sizeof(struct pollfd) * 1);  // check only one fd
  fds[0].fd = getFd();
  fds[0].events = POLLIN | POLLERR | POLLHUP | POLLNVAL;
  int result = poll(fds, 1, timeoutMs);
  if (result == -1) {
    if (errno != EAGAIN && errno != EINTR) {
      cleanupAndReportError("poll", __FUNCTION__, errno);
    }
  } else if (result == 0) {
    // do nothing
  } else {
    assert(result == 1);  // as we check only one fd
    if (fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
      int ofd = getFd();
      this->close();
      LOG_ERROR(
          ERRCODE_INTERCONNECT_ERROR,
          "socket connection failure is encountered detected by poll() fd %d",
          ofd);
    }
    if (fds[0].revents & POLLIN) {
      processRecving();
    } else {  // end of if (fds[0].revents & POLLIN)
      assert("unexpected poll revent" && false);
    }
  }
  return recvMessageLeftSize;
}

// timeout recv, return left content size to receive
void SocketTcpMsgConnection::processRecving() {
  if (recvIsMessageHeader) {
    std::lock_guard<std::mutex> guard(fdMutex);
    int ret = ::recv(fd,
                     reinterpret_cast<char *>(&recvMessageHeader) +
                         sizeof(MessageHeader) - recvMessageLeftSize,
                     recvMessageLeftSize, 0);
    if (ret == 0) {
      this->closeWithoutMutexLocked();
    } else if (ret > 0) {
      recvMessageLeftSize -= ret;
      if (recvMessageLeftSize == 0 && recvIsMessageHeader) {
        recvIsMessageHeader = false;
        recvMessageLeftSize = recvMessageHeader.userMessageLength;
        *recvMessageId = recvMessageHeader.userMessageId;
        recvMessage->resize(recvMessageLeftSize);
        // finished header, so reset counter and set received message id and
        // set expected message content length
      }
    } else {
      if (errno != EINTR && errno != EWOULDBLOCK && errno != ETIMEDOUT) {
        cleanupAndReportErrorWithoutMutexLocked("recv", __FUNCTION__, errno);
      }
    }
    return;
  }

  // receive message content
  int ret = 0;
  {
    std::lock_guard<std::mutex> guard(fdMutex);
    ret = ::recv(fd,
                 const_cast<char *>(recvMessage->data()) +
                     recvMessageHeader.userMessageLength - recvMessageLeftSize,
                 recvMessageLeftSize, 0);
  }
  if (ret == 0) {
    this->close();
  } else if (ret > 0) {
    recvMessageLeftSize -= ret;
  } else {
    if (errno != EINTR && errno != EWOULDBLOCK && errno != ETIMEDOUT) {
      cleanupAndReportError("recv", __FUNCTION__, errno);
    }
  }
}

// sync send, if cancel mark instance is provided, user can cancel sending
// through an async way
void SocketTcpMsgConnection::send(uint8_t messageId, const char *message,
                                  uint32_t messageLength,
                                  AsyncCancelMark *cancelMark) {
  prepareSend(messageId, message, messageLength);
  while (!cancelMark->isCancelled() && isConnected()) {
    if (timeoutSend() == 0) {
      break;
    }
  }
}

// sync recv, if cancel mark instance is provided, user can cancel receiving
// through an async way
void SocketTcpMsgConnection::recv(uint8_t *messageId, std::string *message,
                                  AsyncCancelMark *cancelMark) {
  prepareRecv(messageId, message);
  while (!cancelMark->isCancelled() && isConnected()) {
    if (timeoutRecv() == 0) {
      break;
    }
  }
}

}  // namespace dbcommon
