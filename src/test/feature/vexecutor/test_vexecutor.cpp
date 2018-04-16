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

class TestVexecutor : public ::testing::Test
{
  public:
	TestVexecutor(){};
	~TestVexecutor(){};
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

TEST_F(TestVexecutor, scanframework)
{
	hawq::test::SQLUtility util;

	util.execute("drop table if exists test1");
	util.execute("create table test1 ("
				 "	unique1		int4,"
				 "	unique2		int4,"
				 "	two			int4,"
				 "	four		int4,"
				 "	ten			int4,"
				 "	twenty		int4,"
				 "	hundred		int4,"
				 "	thousand	int4,"
				 "	twothousand	int4,"
				 "	fivethous	int4,"
				 "	tenthous	int4,"
				 "	odd			int4,"
				 "	even		int4,"
				 "	stringu1	name,"
				 "	stringu2	name,"
				 "	string4		name) WITH (appendonly = true, orientation = PARQUET, pagesize = 1048576, rowgroupsize = 8388608, compresstype = SNAPPY) DISTRIBUTED RANDOMLY;");

  std::string pwd = util.getTestRootPath();
  std::string cmd = "COPY test1 FROM '" + pwd + "/vexecutor/data/tenk.data'";
  std::cout << cmd << std::endl;
  util.execute(cmd);
  util.execute("select unique1 from test1");
  util.execute("SET vectorized_executor_enable to on");

  util.execSQLFile("vexecutor/sql/scan1.sql",
					"vexecutor/ans/scan1.ans");

  util.execute("drop table test1");
}

TEST_F(TestVexecutor, scanAO)
{
	hawq::test::SQLUtility util;

	util.execute("drop table if exists test1");
	util.execute("create table test1 ("
				 "	unique1		int4,"
				 "	unique2		int4,"
				 "	two			int4,"
				 "	four		int4,"
				 "	ten			int4,"
				 "	twenty		int4,"
				 "	hundred		int4,"
				 "	thousand	int4,"
				 "	twothousand	int4,"
				 "	fivethous	int4,"
				 "	tenthous	int4,"
				 "	odd			int4,"
				 "	even		int4,"
				 "	stringu1	name,"
				 "	stringu2	name,"
				 "	string4		name) WITH (appendonly = true, compresstype = SNAPPY) DISTRIBUTED RANDOMLY;");

  std::string pwd = util.getTestRootPath();
  std::string cmd = "COPY test1 FROM '" + pwd + "/vexecutor/data/tenk.data'";
  std::cout << cmd << std::endl;
  util.execute(cmd);
  util.execute("select unique1 from test1");
  util.execute("SET vectorized_executor_enable to on");

  util.execSQLFile("vexecutor/sql/scan1.sql",
					"vexecutor/ans/scan1.ans");

  util.execute("drop table test1");
}

TEST_F(TestVexecutor, ProjAndQual)
{
	hawq::test::SQLUtility util;

	util.execute("drop table if exists test1");
	util.execute("create table test1 (a int, b int, c int, d int);");
	util.execute("insert into test1 select generate_series(1,1024), 1, 1, 1;");

	util.execSQLFile("vexecutor/sql/projandqual.sql",
					"vexecutor/ans/projandqual.ans");

	util.execute("SET vectorized_executor_enable to on");

	util.execSQLFile("vexecutor/sql/projandqual.sql",
					"vexecutor/ans/projandqual.ans");

	util.execute("drop table test1");
}
