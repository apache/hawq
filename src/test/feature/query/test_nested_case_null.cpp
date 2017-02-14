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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <iostream>

#include "lib/command.h"
#include "lib/data_gen.h"
#include "lib/hawq_config.h"
#include "lib/sql_util.h"

#include "gtest/gtest.h"

class TestQueryNestedCaseNull : public ::testing::Test {
 public:
  TestQueryNestedCaseNull() {}
  ~TestQueryNestedCaseNull() {}
};

TEST_F(TestQueryNestedCaseNull, Test1) {
  hawq::test::SQLUtility util;

  // prepare
  util.execute("DROP TABLE IF EXISTS t CASCADE");

  // test
  util.execute("CREATE TABLE t(pid INT, wid INT, state CHARACTER VARYING(30))");
  util.execute("INSERT INTO t VALUES(1,1)");
  util.query("SELECT DECODE(DECODE(state, '', NULL, state), '-', NULL, state) AS state FROM t",
             "|\n");

  // cleanup
  util.execute("DROP TABLE t");
}
