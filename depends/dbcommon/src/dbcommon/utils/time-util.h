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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_TIME_UTIL_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_TIME_UTIL_H_

#include <sys/time.h>
#include <time.h>
#include <string>

namespace dbcommon {

class TimeUtil {
 public:
  TimeUtil() {}
  virtual ~TimeUtil() {}

  static uint64_t currentTime() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000LL + tv.tv_usec;
  }

  static uint64_t currentTimeMilliSec() { return currentTime() / 1000LL; }

  static std::string currentTimestampWithTimezone() {
    struct timeval tv;
    struct tm* tm;
    char tmbuf[64];
    gettimeofday(&tv, NULL);
    tm = localtime(&tv.tv_sec);  // NOLINT
    strftime(tmbuf, sizeof tmbuf, "%Y-%m-%d %H:%M:%S", tm);
    return std::string(tmbuf);
  }

  static void usleep(uint64_t usec) {
    struct timeval tv;
    tv.tv_sec = usec / 1000000;
    tv.tv_usec = usec % 1000000;
    select(0, nullptr, nullptr, nullptr, &tv);
  }
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_TIME_UTIL_H_
