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

#include "dbcommon/log/debug-logger.h"

namespace dbcommon {
thread_local int LogTestingGuard::LogTestingLevel_ = 0;
thread_local bool LogTestingGuard::enableLogTesting_ = false;

thread_local int LogScopeTimeGuard::ScopeTimingLevel_ = 0;

LogTestingGuard::LogTestingGuard(std::string title) {
  LogTestingLevel_++;
  if (enableLogTesting_) {
    for (int i = 0; i < LogTestingLevel_; i++) std::cerr << '\t';
    std::cerr << "\033[0;32m["
              << " \033[1;33mTesting: \033[0m" << title
              << " \033[0;32m]\n\033[0m";
  }
}

LogScopeTimeGuard::~LogScopeTimeGuard() {
  ScopeTimingLevel_--;
  auto endTime = std::chrono::system_clock::now();
  std::time_t start = std::chrono::system_clock::to_time_t(startTime_);
  std::time_t end = std::chrono::system_clock::to_time_t(endTime);

  char buffer[10];
  std::stringstream ss;

  // Print start time
  strftime(buffer, 10, "%T.", localtime(&start));
  for (int i = 0; i < ScopeTimingLevel_; i++) ss << '\t';
  ss << "\033[1;31m" << buffer << std::setfill('0') << std::setw(6)
     << std::chrono::time_point_cast<std::chrono::microseconds>(startTime_)
                .time_since_epoch()
                .count() %
            1000000
     << "\033[0m";

  // Print elapse time
  auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
      endTime - startTime_);
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      endTime - startTime_);
  auto elapsed_s = std::chrono::duration_cast<std::chrono::duration<double>>(
      endTime - startTime_);
  ss << " Timing \033[1;33m" << title_ << " : \033[1;32m" << elapsed_us.count()
     << "us " << elapsed_ms.count() << "ms " << elapsed_s.count() << "s "
     << "\033[0m" << std::endl;

  // Print end time
  strftime(buffer, 10, "%T.", localtime(&end));
  for (int i = 0; i < ScopeTimingLevel_; i++) ss << '\t';
  ss << "\033[1;31m" << buffer << std::setfill('0') << std::setw(6)
     << std::chrono::time_point_cast<std::chrono::microseconds>(endTime)
                .time_since_epoch()
                .count() %
            1000000
     << "\033[0m" << std::endl;
  ss << std::endl;

  // Flush out
  std::cerr << ss.str();
}

}  // namespace dbcommon
