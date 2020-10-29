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

// LOG Interface
// Usage:
//      1) Call InitGoogleLogging() at the begin of the main
//      2) use LOG(severity, fmt, ...) to write log, ERROR and FATAL level will
//      throw TransactionAbortException
//      3) other functionality in glog is available too.
//

#ifndef DBCOMMON_SRC_DBCOMMON_LOG_LOGGER_H_
#define DBCOMMON_SRC_DBCOMMON_LOG_LOGGER_H_

#include <cinttypes>
#include <string>

#include "glog/logging.h"

#include "dbcommon/log/error-code.h"
#include "dbcommon/log/exception.h"
#include "dbcommon/log/stack-printer.h"

namespace dbcommon {

#define THREAD_LOCAL __thread
#define STACK_DEPTH (64)

extern THREAD_LOCAL char *threadIdentifier;

#define THREAD_SET_IDENTIFER(identifier) \
  threadIdentifier = const_cast<char *>(identifier);

static std::string getThreadIdentifier() {
  if (threadIdentifier != NULL) {
    std::string id = "thread: " + std::string(threadIdentifier) + " ";
    return id;
  } else {
    std::string a("");
    return a;
  }
}

std::string FormatErrorString(const char *fmt, ...)
    __attribute__((format(printf, 1, 2)));

#define PGSIXBIT(ch) (((ch) - '0') & 0x3F)

#define MAKE_SQLSTATE(ch1, ch2, ch3, ch4, ch5)                    \
  (PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
   (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))  // NOLINT

// xxx: this is only a tricky workaround implementation on LOG_DEBUG
#ifndef NDEBUG
#define LOG_DEBUG(...) COMPACT_LOG_INFO(__VA_ARGS__)
#else
#define LOG_DEBUG(...) \
  {}
#endif
#define LOG_EXIT(...) COMPACT_LOG_WARNING_AND_EXIT(__VA_ARGS__)
#define LOG_INFO(...) COMPACT_LOG_INFO(__VA_ARGS__)
#define LOG_WARNING(...) COMPACT_LOG_WARNING(__VA_ARGS__)
#define LOG_ERROR(errCode, ...) COMPACT_LOG_ERROR(errCode, __VA_ARGS__)
#define LOG_BACKSTRACE(...) COMPACT_LOG_BACKSTRACE(__VA_ARGS__)
#define LOG_NOT_RETRY_ERROR(errCode, ...) \
  COMPACT_LOG_NOT_RETRY_ERROR(errCode, __VA_ARGS__)
#define LOG_FATAL(errCode, ...) COMPACT_LOG_FATAL(errCode, __VA_ARGS__)
#define DLOG_INFO(...) COMPACT_DLOG_INFO(__VA_ARGS__)

#define COMPACT_LOG_FATAL(errCode, ...)                                      \
  do {                                                                       \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__);            \
    COMPACT_GOOGLE_LOG_FATAL.stream()                                        \
        << dbcommon::getThreadIdentifier() << "errCode: " << errCode << ", " \
        << __msg;                                                            \
  } while (0)

#define COMPACT_LOG_ERROR(errCode, ...)                                      \
  do {                                                                       \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__);            \
    COMPACT_GOOGLE_LOG_ERROR.stream()                                        \
        << dbcommon::getThreadIdentifier() << "errCode: " << errCode << ", " \
        << __msg << "\n"                                                     \
        << dbcommon::PrintStack(0, STACK_DEPTH);                             \
    throw dbcommon::TransactionAbortException(__msg, errCode, true);         \
  } while (0)

#define COMPACT_LOG_BACKSTRACE(...)                               \
  do {                                                            \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__); \
    COMPACT_GOOGLE_LOG_ERROR.stream()                             \
        << dbcommon::PrintStack(0, STACK_DEPTH)                   \
        << dbcommon::getThreadIdentifier() << __msg;              \
  } while (0)

#define COMPACT_LOG_NOT_RETRY_ERROR(errCode, ...)                            \
  do {                                                                       \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__);            \
    COMPACT_GOOGLE_LOG_ERROR.stream()                                        \
        << dbcommon::getThreadIdentifier() << "errCode: " << errCode << ", " \
        << __msg << "\n"                                                     \
        << dbcommon::PrintStack(0, STACK_DEPTH);                             \
    throw dbcommon::TransactionAbortException(__msg, errCode, false);        \
  } while (0)

#define COMPACT_LOG_WARNING(...)                                  \
  do {                                                            \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__); \
    COMPACT_GOOGLE_LOG_WARNING.stream()                           \
        << dbcommon::getThreadIdentifier() << __msg;              \
  } while (0)

#define COMPACT_LOG_WARNING_AND_EXIT(...)                                    \
  do {                                                                       \
    FLAGS_logtostderr = 1;                                                   \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__);            \
    COMPACT_GOOGLE_LOG_WARNING.stream()                                      \
        << "exit with error : " << dbcommon::getThreadIdentifier() << __msg; \
    FLAGS_logtostderr = 0;                                                   \
    exit(1);                                                                 \
  } while (0)

#define COMPACT_LOG_INFO(...)                                     \
  do {                                                            \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__); \
    COMPACT_GOOGLE_LOG_INFO.stream()                              \
        << dbcommon::getThreadIdentifier() << __msg;              \
  } while (0)

#define COMPACT_DLOG_INFO(...)                                    \
  do {                                                            \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__); \
    DLOG(INFO) << dbcommon::getThreadIdentifier() << __msg;       \
  } while (0)

class LogTool {
 public:
  LogTool() {
    ::google::InitGoogleLogging("");
    ::google::LogToStderr();
  }
  ~LogTool() { ::google::ShutdownGoogleLogging(); }
};

}  // end of namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_LOG_LOGGER_H_
