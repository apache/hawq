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


class TestPreparedStatement: public ::testing::Test
{
	public:
		TestPreparedStatement() {}
		~TestPreparedStatement() {}
};

// HAWQ-800: https://issues.apache.org/jira/browse/HAWQ-800
// HAWQ-835: https://issues.apache.org/jira/browse/HAWQ-835
TEST_F(TestPreparedStatement, TestPreparedStatementPrepare)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("PreparedStatement/sql/proba.sql",
	                 "PreparedStatement/ans/proba.ans");
}

// HAWQ-800: https://issues.apache.org/jira/browse/HAWQ-800
// HAWQ-835: https://issues.apache.org/jira/browse/HAWQ-835
TEST_F(TestPreparedStatement, TestPreparedStatementExecute)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("PreparedStatement/sql/proba_execute.sql",
	                 "PreparedStatement/ans/proba_execute.ans");
}

// HAWQ-800: https://issues.apache.org/jira/browse/HAWQ-800
// HAWQ-835: https://issues.apache.org/jira/browse/HAWQ-835
TEST_F(TestPreparedStatement, TestPreparedStatementInsert)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("PreparedStatement/sql/insert.sql",
	                 "PreparedStatement/ans/insert.ans");
}
