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

#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestLock: public ::testing::Test
{
	public:
		TestLock() { }
		~TestLock() {}
};

TEST_F(TestLock, LockBasics)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("lock/sql/lock.sql", "lock/ans/lock.ans");
}

TEST_F(TestLock, DeadLock)
{
  hawq::test::SQLUtility util1(hawq::test::MODE_DEFAULT);
  hawq::test::SQLUtility util2(hawq::test::MODE_DEFAULT);
  util1.execute("drop table if exists TEST1");
  util1.execute("drop table if exists TEST2");
  util1.execute("CREATE TABLE TEST1(A INT)");
  util1.execute("CREATE TABLE TEST2(A INT)");
  util1.execute("insert into TEST1 values(1)");
  util1.execute("insert into TEST2 values(1)");
  util1.execute("begin");
  util1.execute("TRUNCATE TEST1");
  util2.execute("begin");
  util2.execute("TRUNCATE TEST2");
  util1.execute("TRUNCATE TEST2");
  util2.execute("TRUNCATE TEST1");
  util1.execute("TRUNCATE TEST2");
  util1.execute("end");
  util2.execute("end");
  util1.query("select * from TEST1",0);
  util1.query("select * from TEST2",0);
}

