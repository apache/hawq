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

#include "lib/data_gen.h"
#include "lib/sql_util.h"

using hawq::test::SQLUtility;

class TestParquet : public ::testing::Test {
 public:
  TestParquet() {}
  ~TestParquet() {}
};

TEST_F(TestParquet, TestMultipleType) {
  SQLUtility util;
  util.execute("drop table if exists t1");
  hawq::test::DataGenerator dGen(&util);
  dGen.genTableWithFullTypes("t1", true, "parquet");
  util.query("select * from t1", 6);
}

TEST_F(TestParquet, TestCompression) {
  SQLUtility util;
  util.execute("drop table if exists t21");
  util.execute("drop table if exists t22");
  hawq::test::DataGenerator dGen(&util);
  dGen.genTableWithNull("t21", true, "parquet", "gzip", 9);
  dGen.genTableWithNull("t22", true, "parquet", "snappy");
  util.query(
      "select * from t21,t22 order by t21.a,t21.b,t21.c,t22.a,t22.b,t22.c",
      "15||aa|15||aa|\n15||aa||51||\n15||aa|||WET|\n|51||15||aa|\n|51|||51||\n|"
      "51||||WET|\n||WET|15||aa|\n||WET||51||\n||WET|||WET|\n");
}

TEST_F(TestParquet, TestSize) {
  SQLUtility util;
  // value/record size equal to pagesize/rowgroupsize
  util.execute("drop table if exists t31");
  util.execute(
      "create table t31 (a1 char(10485760), a2 char(10485760), a3 "
      "char(10485760), a4 char(10485760), a5 char(10485760), a6 "
      "char(10485760), a7 char(10485760), a8 char(10485760), a9 "
      "char(10485760), a10 char(10485760)) with(appendonly=true, "
      "orientation=parquet, pagesize=10485760, rowgroupsize=104857600)");
  util.execute(
      "insert into t31 values ( ('a'::char(10485760)), "
      "('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), "
      "('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)), "
      "('a'::char(10485760)), ('a'::char(10485760)), ('a'::char(10485760)) );",
      false);
  EXPECT_STREQ("ERROR:  value for column \"a1\" exceeds pagesize 10485760!",
               util.getPSQL()->getLastResult().substr(0, 56).c_str());

  // single column, one data page contains several values, one rwo group
  // contains several groups
  util.execute("drop table if exists t32");
  util.execute("drop table if exists t33");
  util.execute(
      "create table t32 ( a1 text ) with(appendonly=true, "
      "orientation=parquet)");
  util.execute("insert into t32 values(repeat('parquet',100))");
  util.execute("insert into t32 values(repeat('parquet',20))");
  util.execute("insert into t32 values(repeat('parquet',30))");
  util.execute(
      "create table t33 ( a1 text ) with(appendonly=true, orientation=parquet, "
      "pagesize=1024, rowgroupsize=1025)");
  util.execute("insert into t33 select * from t32");
  util.query("select * from t33", 3);

  // large data insert, several column values in one page, several rows in one
  // rowgroup
  util.execute("drop table if exists t34");
  util.execute("drop table if exists t35");
  util.execute(
      "create table t34 (a1 char(1048576), a2 char(2048576), a3 "
      "char(3048576), a4 char(4048576), a5 char(5048576), a6 char(6048576), a7 "
      "char(7048576), a8 char(8048576), a9 char(9048576), a10 char(9)) "
      "with(appendonly=true, orientation=parquet, pagesize=10485760, "
      "rowgroupsize=90874386)");
  util.execute(
      "insert into t34 values ( ('a'::char(1048576)), "
      "('a'::char(2048576)), ('a'::char(3048576)), ('a'::char(4048576)), "
      "('a'::char(5048576)), ('a'::char(6048576)), ('a'::char(7048576)), "
      "('a'::char(8048576)), ('a'::char(9048576)), ('a'::char(9)) )");
  util.execute(
      "create table t35 (a1 char(1048576), a2 char(2048576), a3 "
      "char(3048576), a4 char(4048576), a5 char(5048576), a6 char(6048576), a7 "
      "char(7048576), a8 char(8048576), a9 char(9048576), a10 char(9)) "
      "with(appendonly=true, orientation=parquet, pagesize=10485760, "
      "rowgroupsize=17437200)");
  util.execute("insert into t35 select * from t34");
  util.query("select * from t35", 1);
}

TEST_F(TestParquet, TestPartition) {
  SQLUtility util;
  util.execute("drop table if exists t4");
  util.execute(
      "create table t4 (id SERIAL,a1 int,a2 char(5)) WITH (appendonly=true, "
      "orientation=parquet,compresstype=gzip,compresslevel=1) distributed "
      "randomly Partition by range(a1) (start(1)  end(16) every(8)"
      "WITH(appendonly = true, orientation = parquet, compresstype = "
      "snappy))");
  util.execute("insert into t4(a1,a2) values(generate_series(1,5),'M')");
  util.execute(
      "alter table t4 add partition new_p start(17) end (20) WITH "
      "(appendonly=true, orientation=parquet, compresstype=snappy)");
  util.execute(
      "alter table t4 add default partition df_p WITH "
      "(appendonly=true, orientation=parquet, compresstype=gzip, "
      "compresslevel=3)");
  util.execute("insert into t4(a1,a2) values(generate_series(6,25),'F')");
  util.query("select * from t4", 25);
}
