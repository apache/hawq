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

#include "gtest/gtest.h"

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/string-util.h"

TEST(TestStringUtil, BasicTest) {
  using dbcommon::StringUtil;

  std::string tstring = "tSStDIR";
  StringUtil::toLower(&tstring);

  EXPECT_EQ(tstring, "tsstdir");

  std::string empty;
  EXPECT_EQ(StringUtil::trim(empty), "");

  EXPECT_EQ(StringUtil::StartWith(empty, empty), false);
  EXPECT_EQ(StringUtil::StartWith(tstring, empty), false);

  std::string testString = "TyyA";
  EXPECT_EQ(StringUtil::StartWith(testString, "Ty"), true);
}
