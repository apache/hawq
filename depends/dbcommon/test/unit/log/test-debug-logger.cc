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

#define LOG_COLOR
#define LOG_TIMING

#include "dbcommon/log/debug-logger.h"
#include "gtest/gtest.h"

namespace dbcommon {

TEST(TestLogColor, Test) {
  LOG_RED("Log red number %d", 38324);
  LOG_YELLOW("Log yellow string %s", "14122");
  LOG_GREEN("Log green");
}

TEST(TestLogStack, Test) { LOG_STACK("TestLogStack"); }

TEST(TestLogScopeTime, Test) {
  LOG_SCOPE_TIME(__PRETTY_FUNCTION__);
  { LOG_SCOPE_TIME("Log inner time"); }
}

}  // namespace dbcommon
