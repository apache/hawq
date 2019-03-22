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

#include "dbcommon/log/logger.h"

#include <stdarg.h>

#include <vector>

namespace dbcommon {

THREAD_LOCAL char *threadIdentifier = NULL;

std::string FormatErrorString(const char *fmt, ...) {
  std::vector<char> logBuffer;
  va_list ap;

  va_start(ap, fmt);
  int n = vsnprintf(reinterpret_cast<char *>(logBuffer.data()), 0, fmt, ap);
  va_end(ap);

  logBuffer.resize(n + 1);

  va_start(ap, fmt);
  vsnprintf(reinterpret_cast<char *>(logBuffer.data()), n + 1, fmt, ap);
  va_end(ap);

  return std::string(reinterpret_cast<char *>(logBuffer.data()));
}

}  // end of namespace dbcommon
