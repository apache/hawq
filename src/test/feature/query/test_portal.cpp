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

class TestQueryPortal : public ::testing::Test {
 public:
  TestQueryPortal() {}
  ~TestQueryPortal() {}
};

TEST_F(TestQueryPortal, TestBasic1) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("ALTER RESOURCE QUEUE pg_default WITH (active_statements=30)");
  util.execute("DROP TABLE IF EXISTS test1 CASCADE");
  util.execute("DROP TABLE IF EXISTS test2 CASCADE");
  util.execute("CREATE TABLE test1 (a INT, b INT, c INT, d INT)");
  util.execute("CREATE TABLE test2 (a INT, b INT, c INT, d INT)");
  util.execute("INSERT INTO test1 SELECT x, 2*x, 3*x, 4*x FROM generate_series(1,1000) x");
  util.execute("INSERT INTO test2 SELECT x, 2*x, 3*x, 4*x FROM generate_series(1,1000) x");

  // test
  util.execSQLFile("query/sql/portal-basic.sql",
		  	  	   "query/ans/portal-basic.ans");

  // clean up
  util.execute("DROP TABLE test1");
  util.execute("DROP TABLE test2");
  util.execute("ALTER RESOURCE QUEUE pg_default WITH (active_statements=20)");
}
