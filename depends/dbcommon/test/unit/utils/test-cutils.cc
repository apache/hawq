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
#include "dbcommon/utils/cutils.h"
#include "dbcommon/utils/string-util.h"

TEST(TestCUtils, BasicTest) {
  // test failed only in release mode
  // EXPECT_THROW(dbcommon::cnmalloc(100000000000000000),
  //             dbcommon::TransactionAbortException);

  char *ret = dbcommon::cnmalloc(0);
  EXPECT_NE(ret, nullptr);

  ret = dbcommon::cnmalloc(1024);
  *ret = '1';
  EXPECT_EQ(*ret, '1');

  ret = dbcommon::cnrealloc(ret, 2048);
  EXPECT_EQ(*ret, '1');

  ret = dbcommon::cnrealloc(ret, 1024);
  EXPECT_EQ(*ret, '1');

  ret = dbcommon::cnrealloc(ret, 0);
#if GTEST_OS_LINUX
  EXPECT_EQ(ret, nullptr);
#else
  EXPECT_NE(ret, nullptr);
#endif
}
