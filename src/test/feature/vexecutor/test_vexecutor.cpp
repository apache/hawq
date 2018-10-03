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


TEST_F(TestVexecutor, vadt)
{
	// preprocess source files to get sql/ans files
	hawq::test::SQLUtility util;

	util.execSQLFile("vexecutor/sql/create_type.sql",
					 "vexecutor/ans/create_type_vadt.ans");

	util.execSQLFile("vexecutor/sql/vadt.sql",
					 "vexecutor/ans/vadt.ans");

	// run sql file to get ans file and then diff it with out file
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

	util.execSQLFile("vexecutor/sql/create_type.sql");

	util.execute("select unique1 from test1");
	util.execute("SET vectorized_executor_enable to on");

	util.execSQLFile("vexecutor/sql/scan1.sql",
					"vexecutor/ans/scan1.ans");

	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("vexecutor/sql/drop_type.sql");

	util.execute("drop table test1");
};


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
	util.execSQLFile("vexecutor/sql/create_type.sql");

	util.execute("select unique1 from test1");
	util.execute("SET vectorized_executor_enable to on");

	util.execSQLFile("vexecutor/sql/scan1.sql",
					"vexecutor/ans/scan1.ans");

	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("vexecutor/sql/drop_type.sql");

	util.execute("drop table test1");
};

TEST_F(TestVexecutor, ProjAndQual)
{
	hawq::test::SQLUtility util;

	util.execute("drop table if exists test1");
	util.execute("create table test1 (a int, b int, c int, d int);");
	util.execute("insert into test1 select generate_series(1,1024), 1, 1, 1;");

	util.execSQLFile("vexecutor/sql/create_type.sql");

	util.execSQLFile("vexecutor/sql/projandqual.sql",
					"vexecutor/ans/projandqual.ans");

	util.execute("SET vectorized_executor_enable to on");

	util.execSQLFile("vexecutor/sql/projandqual.sql",
					"vexecutor/ans/projandqual.ans");

	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("vexecutor/sql/drop_type.sql");

	util.execute("drop table test1");
};

TEST_F(TestVexecutor, date)
 {
 	hawq::test::SQLUtility util;
 	util.execute("drop table if exists test1");
 	util.execute("create table test1 (a date,i int) WITH (appendonly = true, compresstype = SNAPPY) DISTRIBUTED RANDOMLY;");
 	util.execute("insert into test1 select '1998-03-28'::date + generate_series(1,2000), 1;");
 	util.execute("select a + i from test1;");

 	util.execSQLFile("vexecutor/sql/create_type.sql");
 
 	util.execute("SET vectorized_executor_enable to on");
 
 	util.execSQLFile("vexecutor/sql/vdate.sql",
 					"vexecutor/ans/vdate.ans");
 
	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("vexecutor/sql/drop_type.sql");

 	util.execute("drop table test1");
 }

TEST_F(TestVexecutor, vagg)
{
	hawq::test::SQLUtility util;

	util.execute("create table test_int2(a int2, b int2, c int2)");
	util.execute("create table test_int4(a int4, b int4, c int4)");
	util.execute("create table test_int8(a int8, b int8, c int8)");
	util.execute("create table test_float4(a float4, b float4, c float4)");
	util.execute("create table test_float8(a float8, b float8, c float8)");

	util.execute("insert into test_int2 select generate_series(1,16),1,2");
	util.execute("insert into test_int4 select generate_series(1,16),1,2");
	util.execute("insert into test_int8 select generate_series(1,16),1,2");
	util.execute("insert into test_float4 select generate_series(1,16),1,2");
	util.execute("insert into test_float8 select generate_series(1,16),1,2");

	util.execute("insert into test_int2 select null, null, null");
	util.execute("insert into test_int4 select null, null, null");
	util.execute("insert into test_int8 select null, null, null");
	util.execute("insert into test_float4 select null, null, null");
	util.execute("insert into test_float8 select null, null, null");


	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("vexecutor/sql/create_type.sql");

	util.execSQLFile("vexecutor/sql/vagg.sql",
					"vexecutor/ans/vagg.ans");

	util.execute("SET vectorized_executor_enable to on");
	util.execute("SET vectorized_batch_size = 4");

	util.execSQLFile("vexecutor/sql/vagg.sql",
					"vexecutor/ans/vagg.ans");

	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("vexecutor/sql/drop_type.sql");

	util.execute("drop table if exists test_int2");
	util.execute("drop table if exists test_int4");
	util.execute("drop table if exists test_int8");
	util.execute("drop table if exists test_float4");	
	util.execute("drop table if exists test_float8");
};


