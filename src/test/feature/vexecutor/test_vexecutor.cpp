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

class TestVexecutor: public ::testing::Test
{
	public:
		TestVexecutor() {};
		~TestVexecutor() {};
};

//#define TEST_F_FILE(TestName, basePath, testcase)	\
//TEST_F(TestName, testcase)							\
//{													\
//	hawq::test::SQLUtility util;					\
//	string SqlFile(basePath);						\
//	string AnsFile(basePath);						\
//	SqlFile += "/sql/" #testcase ".sql";			\
//	AnsFile += "/ans/" #testcase ".ans";			\
//	util.execSQLFile(SqlFile, AnsFile);				\
//}
//
//#define TEST_F_FILE_TYPE(testcase) TEST_F_FILE(TestVexecutor, "vexecutor", testcase)
//TEST_F_FILE_TYPE(create_type)
//TEST_F_FILE_TYPE(vadt)
//TEST_F_FILE_TYPE(drop_type)


TEST_F(TestVexecutor, vadt)
{
	// preprocess source files to get sql/ans files
	hawq::test::SQLUtility util;

	util.execute("drop type vint2 cascade", false);
	util.execute("drop type vint4 cascade", false);
	util.execute("drop type vint8 cascade", false);
	util.execute("drop type vfloat4 cascade", false);
	util.execute("drop type vfloat8 cascade", false);
	util.execute("drop type vbool cascade", false);

	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("vexecutor/sql/create_type.sql",
	                 "vexecutor/ans/create_type.ans");

	util.execSQLFile("vexecutor/sql/vadt.sql",
	                 "vexecutor/ans/vadt.ans");

	util.execSQLFile("vexecutor/sql/drop_type.sql",
	                 "vexecutor/ans/drop_type.ans");
}
