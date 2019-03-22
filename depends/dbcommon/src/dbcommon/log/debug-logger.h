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

#ifndef DBCOMMON_SRC_DBCOMMON_LOG_DEBUG_LOGGER_H_
#define DBCOMMON_SRC_DBCOMMON_LOG_DEBUG_LOGGER_H_

#include <chrono>  // NOLINT
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "dbcommon/log/logger.h"

namespace dbcommon {

class LogTestingGuard {
 public:
  explicit LogTestingGuard(std::string title);
  ~LogTestingGuard() { LogTestingLevel_--; }

  static void enableLogTesting() { enableLogTesting_ = true; }

  static void disableLogTesting() { enableLogTesting_ = false; }

 private:
  static thread_local int LogTestingLevel_;
  static thread_local bool enableLogTesting_;
};

class LogScopeTimeGuard {
 public:
  explicit LogScopeTimeGuard(const std::string &title) : title_(title) {
    startTime_ = std::chrono::system_clock::now();
    ScopeTimingLevel_++;
  }
  ~LogScopeTimeGuard();

 private:
  static thread_local int ScopeTimingLevel_;

  std::string title_;
  std::chrono::system_clock::time_point startTime_;
};

}  // namespace dbcommon

/*
 * The following macro is used to log the call stack, log with color in terminal
 * Use them only when debugging
 */
#if defined(LOG_COLOR) && !defined(NO_LOG_COLOR)
#define LOG_STACK(...)                                            \
  do {                                                            \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__); \
    COMPACT_GOOGLE_LOG_ERROR.stream()                             \
        << __func__ << " \033[1;36m" + __msg + "\033[0m\n"        \
        << dbcommon::PrintStack(0, STACK_DEPTH);                  \
  } while (0)

#define LOG_RED(...)                                              \
  do {                                                            \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__); \
    COMPACT_GOOGLE_LOG_ERROR.stream()                             \
        << __func__ << " \033[1;31m" + __msg + "\033[0m";         \
  } while (0)

#define LOG_GREEN(...)                                            \
  do {                                                            \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__); \
    COMPACT_GOOGLE_LOG_ERROR.stream()                             \
        << __func__ << " \033[1;32m" + __msg + "\033[0m";         \
  } while (0)

#define LOG_YELLOW(...)                                           \
  do {                                                            \
    std::string __msg = dbcommon::FormatErrorString(__VA_ARGS__); \
    COMPACT_GOOGLE_LOG_ERROR.stream()                             \
        << __func__ << " \033[1;33m" + __msg + "\033[0m";         \
  } while (0)
#else
#define LOG_STACK(...) \
  {}
#define LOG_RED(...) \
  {}
#define LOG_GREEN(...) \
  {}
#define LOG_YELLOW(...) \
  {}
#endif

#define LOG_TESTING(...)                     \
  dbcommon::LogTestingGuard logTestingGuard( \
      dbcommon::FormatErrorString(__VA_ARGS__));

#ifdef LOG_TIMING
#define LOG_SCOPE_TIME(TITLE) \
  dbcommon::LogScopeTimeGuard logScopeTimeGuard(TITLE);
#else
#define LOG_SCOPE_TIME(TITLE) \
  {}
#endif

#endif  // DBCOMMON_SRC_DBCOMMON_LOG_DEBUG_LOGGER_H_
