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

class TestRowTypes : public ::testing::Test
{
	public:
		TestRowTypes() { }
		~TestRowTypes() {}
};

TEST_F(TestRowTypes, BasicTest)
{
	hawq::test::SQLUtility util;

    util.execute("drop table if exists tenk1");
    util.execute("create table tenk1 ("
                "    unique1     int4,"
                "    unique2     int4,"
                "    two         int4,"
                "    four        int4,"
                "    ten         int4,"
                "    twenty      int4,"
                "    hundred     int4,"
                "    thousand    int4,"
                "    twothousand int4,"
                "    fivethous   int4,"
                "    tenthous    int4,"
                "    odd         int4,"
                "    even        int4,"
                "    stringu1    name,"
                "    stringu2    name,"
                "    string4     name) with oids");
	
    std::string pwd = util.getTestRootPath();
    std::string cmd = "COPY tenk1 FROM '" + pwd + "/query/data/tenk.data'";
    std::cout << cmd << std::endl;
    util.execute(cmd);
    
    util.execSQLFile("query/sql/rowtypes.sql",
	                 "query/ans/rowtypes.ans");
}
