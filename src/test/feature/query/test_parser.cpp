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
#include "lib/sql_util.h"
#include "gtest/gtest.h"

class TestParser : public ::testing::Test {
 public:
  TestParser() {}
  ~TestParser() {}
};


TEST_F(TestParser, TestParserCaseGroupBy) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS mytable CASCADE");
  util.execute("CREATE TABLE mytable (a int, b int, c varchar(1))");
  // test
  util.execSQLFile("query/sql/parser-casegroupby.sql",
		  	  	   "query/ans/parser-casegroupby.ans");
  util.execute("DROP TABLE mytable CASCADE");
  util.execute("DROP FUNCTION negate(int)");
}

