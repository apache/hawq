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
#include <string>

#include "lib/sql_util.h"

#include "gtest/gtest.h"

class TestQueryInsert : public ::testing::Test {
 public:
  TestQueryInsert() {}
  ~TestQueryInsert() {}
};


TEST_F(TestQueryInsert, TestInsertWithDefault) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists inserttest");
  util.execute("create table inserttest (col1 int4, "
		  	  	  	  	  	  	  	    "col2 int4 not null,"
		  	  	  	  	  	  	  	    "col3 text default 'testing')");
  // test insert
  util.execute("insert into inserttest(col2, col3) values(3,default)");
  util.execute("insert into inserttest(col1, col2, col3) "
		                    "values(default, 5, default)");
  util.execute("insert into inserttest values(default, 5, 'test')");
  util.execute("insert into inserttest values(default, 7)");

  util.query("select col1, col2, col3 from inserttest order by col1, col2, col3",
		     "|3|testing|\n"
		     "|5|test|\n"
		     "|5|testing|\n"
		     "|7|testing|\n");
  // cleanup
  util.execute("drop table if exists inserttest");
}

TEST_F(TestQueryInsert, TestInsertWithDefaultNeg) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists inserttest");
  util.execute("create table inserttest (col1 int4, "
										"col2 int4 not null,"
										"col3 text default 'testing')");
  // test insert
  util.execute("insert into inserttest(col1, col2, col3) "
							"values(default, default, default)",
			   false);
  std::string errstr = "ERROR:  null value in column \"col2\" "
		               "violates not-null constraint";
  EXPECT_STREQ(errstr.c_str(),
               util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());
  // cleanup
  util.execute("drop table if exists inserttest");
}

TEST_F(TestQueryInsert, TestInsertUnmatchedColNumber) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists inserttest");
  util.execute("create table inserttest (col1 int4, "
										"col2 int4 not null,"
										"col3 text default 'testing')");
  // test insert
  util.execute("insert into inserttest(col1, col2, col3) "
						   "values(default, default)",
			   false);
  std::string errstr = "ERROR:  INSERT has more target columns than "
					   "expressions";
  EXPECT_STREQ(errstr.c_str(),
			   util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());

  util.execute("insert into inserttest(col1, col2, col3) values(1, 2)",
			   false);
  EXPECT_STREQ(errstr.c_str(),
			   util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());

  util.execute("insert into inserttest(col1) values(1, 2)", false);
  errstr = "ERROR:  INSERT has more expressions than target columns";
  EXPECT_STREQ(errstr.c_str(),
			   util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());

  util.execute("insert into inserttest(col1) values(default, default)", false);
  EXPECT_STREQ(errstr.c_str(),
			   util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());

  // cleanup
  util.execute("drop table if exists inserttest");

}

TEST_F(TestQueryInsert, TestInsertValues) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists inserttest");
  util.execute("create table inserttest (col1 int4, "
										"col2 int4 not null,"
										"col3 text default 'testing')");
  // test insert
  util.execute("insert into inserttest "
		                   "values(10,20,'40'),"
		                         "(-1,2,default),"
		                         "((select 2), "
		                          "(select i from (values(3)) as foo(i)),"
		                          "'values are fun!')");

  util.query("select col1, col2, col3 from inserttest order by col1, col2, col3",
			 "-1|2|testing|\n"
			 "2|3|values are fun!|\n"
			 "10|20|40|\n");
  // cleanup
  util.execute("drop table if exists inserttest");
}

TEST_F(TestQueryInsert, TestInsertAfterDropColumn) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists bar");
  util.execute("drop table if exists foo");
  util.execute("create table bar(x int, y int) distributed randomly");
  util.execute("create table foo(like bar) distributed randomly");

  // test
  util.execute("alter table foo drop column y");
  util.execute("insert into bar values(1,1),(2,2)");
  util.execute("insert into foo(x) select t1.x from bar t1 join bar t2 on t1.x=t2.x");
  util.execute("insert into foo(x) select t1.x from bar t1");
  util.execute("insert into foo(x) select t1.x from bar t1 group by t1.x");

  // cleanup
  util.execute("drop table if exists foo");
  util.execute("drop table if exists bar");
}

TEST_F(TestQueryInsert, TestInsertAfterAddDropColumn) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists bar");
  util.execute("drop table if exists foo");
  util.execute("create table bar(x int) distributed randomly");
  util.execute("create table foo(like bar) distributed randomly");

  // test add and drop the column at once
  util.execute("alter table foo add column y int default 0");
  util.execute("alter table foo drop column y");
  util.execute("insert into bar values(1),(2)");
  util.execute("insert into foo(x) select t1.x from bar t1 join bar t2 on t1.x=t2.x");
  util.execute("insert into foo(x) select t1.x from bar t1");
  util.execute("insert into foo(x) select t1.x from bar t1 group by t1.x");

  // cleanup
  util.execute("drop table if exists foo");
  util.execute("drop table if exists bar");
}

TEST_F(TestQueryInsert, TestInsertAppendOnlyTrue) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists t");
  util.execSQLFile("query/sql/insert-appendonlytrue.sql",
		  	  	   "query/ans/insert-appendonlytrue.ans");
  // cleanup
  util.execute("drop table if exists t");
}

