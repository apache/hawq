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

class TestAlterTable : public ::testing::Test {
 public:
  TestAlterTable() {}
  ~TestAlterTable() {}
};


TEST_F(TestAlterTable, TestAlterTableAOColumnDefaultValue) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists altable");
  util.execute("create table altable (a int, b text, c int)");
  util.execute("insert into altable "
                 "select i, i::text, i from generate_series(1,10) i");

  // test add new column into an ao table without default value setting
  util.execute("alter table altable add column y int", false);
  std::string errstr = "ERROR:  ADD COLUMN with no default value in "
                       "append-only tables is not yet supported.";
  EXPECT_STREQ(errstr.c_str(),
               util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());

  // test add new column into an ao table having default value setting
  util.execute("alter table altable add column x int default 1");
  util.query("select a,b,c,x from altable where a=1",
             "1|1|1|1|\n");

  // test alter column having default value setting
  util.execute("alter table altable alter column c set default 10 - 1");
  util.execute("insert into altable(a,b) values(11,'11')");
  util.query("select a,b,c from altable where a=11",
             "11|11|9|\n");

  // test alter column dropping default value setting
  util.execute("alter table altable alter column c drop default");
  util.execute("insert into altable(a,b) values(12,'12')");
  util.query("select a,b,c from altable where a=12",
             "12|12||\n");

  // cleanup
  util.execute("drop table altable");
}

TEST_F(TestAlterTable, TestAlterTableAOColumnNOTNULL) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists altable");
  util.execute("create table altable (a int, b text, c int)");
  util.execute("insert into altable "
                 "select i, i::text, i from generate_series(1,10) i");

  // test set not null
  util.execute("alter table altable alter column c set not null");

  util.execute("insert into altable(a,b) values(13,'13')", false);
  std::string errstr = "ERROR:  null value in column \"c\" violates "
                       "not-null constraint";
  EXPECT_STREQ(errstr.c_str(),
               util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());

  // test drop not null
  util.execute("alter table altable alter column c drop not null");
  util.execute("insert into altable(a,b) values(13,'13')");
  util.query("select a,b,c from altable where a=13",
             "13|13||\n");

  // cleanup
  util.execute("drop table altable");
}

TEST_F(TestAlterTable, TestAlterTableAOColumnConstraint) {
  hawq::test::SQLUtility util;
  bool orcaon = false;
  if (util.getGUCValue("optimizer") == "on") {
	std::cout << "NOTE: TestAlterTable.TestAlterTableAOColumnConstraint "
                 "uses answer for optimizer on" << std::endl;
    orcaon = true;
  }
  // prepare
  util.execute("drop table if exists altable");
  util.execute("create table altable (a int, b text, c int)");
  util.execute("insert into altable "
                 "select i, i::text, i from generate_series(1,10) i");

  // test , constaint is broken by existing rows
  util.execute("alter table altable "
                 "add constraint c_check check (c<10)", false);
  std::string errstr = "ERROR:  check constraint \"c_check\" "
                       "is violated by some row";
  EXPECT_STREQ(errstr.c_str(),
               util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());

  // test, new row breaks existing contraint
  util.execute("alter table altable add constraint c_check check (c>0)");
  util.execute("insert into altable(a,b,c) values(11,'11',-11)", false);
  if (orcaon) {
    errstr = "ERROR:  One or more assertions failed";
  }
  else {
    errstr = "ERROR:  new row for relation \"altable\" "
             "violates check constraint \"c_check\"";
  }
  EXPECT_STREQ(errstr.c_str(),
               util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());
  if (orcaon) {
	std::string errdetail = "DETAIL:  Check constraint c_check for table "
                            "altable was violated";
    std::string::size_type find = util.getPSQL()->getLastResult().find(errdetail);
    EXPECT_NE(find, std::string::npos);
  }

  // test, drop constraint
  util.execute("alter table altable drop constraint c_check");
  util.execute("insert into altable(a,b,c) values(11,'11',-11)");
  util.query("select a,b,c from altable where a=11",
             "11|11|-11|\n");
  // cleanup
  util.execute("drop table altable");
}

TEST_F(TestAlterTable, TestAlterTableAOColumnMisc) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists altable");
  util.execute("create table altable (a int, b text, c int)");
  util.execute("insert into altable "
                 "select i, i::text, i from generate_series(1,10) i");

  util.execute("alter table altable alter column c set statistics 100");
  util.execute("alter table altable alter column b set storage plain");
  util.execute("insert into altable(a,b,c) values(11,'11',11)");
  util.query("select a,b,c from altable where a=11",
             "11|11|11|\n");

  // drop column
  util.execute("alter table altable drop column b");
  util.query("select a,c from altable where a=1", "1|1|\n");

  // change column type from int to bigint
  util.execute("alter table altable alter column c type bigint");

  // miscs ( should add more to verify the changes after successfully changed
  // the target table
  util.execute("alter table altable set without oids");

  util.execute("alter table altable set (fillfactor=90)", false);
  std::string errstr = "ERROR:  altering reloptions for append only tables "
                       "is not permitted";
  EXPECT_STREQ(errstr.c_str(),
               util.getPSQL()->getLastResult().substr(0, errstr.size()).c_str());

  // cleanup
  util.execute("drop table altable");
}

TEST_F(TestAlterTable, TestAlterTableAODropColumn) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists altable");
  util.execute("create table altable (a int, b text, c int)");
  util.execute("insert into altable "
                 "select i, i::text, i from generate_series(1,10) i");

  // test set not null
  util.execute("alter table altable drop column b");

  util.query("select a,c from altable where a=10","10|10|\n");

  // cleanup
  util.execute("drop table altable");
}

TEST_F(TestAlterTable, TestAlterTableOwner) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists altable");
  util.execute("drop user if exists altuser");

  // test
  util.execute("create user altuser");
  util.execute("create table altable (a,b) as values(1,10),(2,20)");
  util.execute("alter table altable owner to altuser");
  util.execute("set role altuser");
  util.execute("insert into altable(a,b) values(3,30)");
  util.execute("reset role");

  // cleanup
  util.execute("drop table altable");
  util.execute("drop user altuser");
}

TEST_F(TestAlterTable, TestAlterTableAddColumn) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("drop table if exists tmp");

  // test
  util.execute("create table tmp (initial int4)");
  util.execute("ALTER TABLE tmp ADD COLUMN a int4 default 3");
  util.execute("ALTER TABLE tmp ADD COLUMN b name default 'Alan Turing'");
  util.execute("ALTER TABLE tmp ADD COLUMN c text default 'Pivotal'");
  util.execute("ALTER TABLE tmp ADD COLUMN d float8 default 0");
  util.execute("ALTER TABLE tmp ADD COLUMN e float4 default 0");
  util.execute("ALTER TABLE tmp ADD COLUMN f int2 default 0");
  util.execute("ALTER TABLE tmp ADD COLUMN g polygon default "
                   "'(1,1),(1,2),(2,2)'::polygon");
  util.execute("ALTER TABLE tmp ADD COLUMN h abstime default null");
  util.execute("ALTER TABLE tmp ADD COLUMN i char default 'P'");
  util.execute("set datestyle=ISO,DMY;"
               "ALTER TABLE tmp ADD COLUMN j abstime[] "
                   "default ARRAY['2/2/2013 4:05:06'::abstime, "
                                 "'2/2/2013 5:05:06'::abstime]");
  util.execute("ALTER TABLE tmp ADD COLUMN k int4 default 0");
  util.execute("ALTER TABLE tmp ADD COLUMN l tid default '(0,1)'::tid");
  util.execute("ALTER TABLE tmp ADD COLUMN m xid default '0'::xid");
  util.execute("ALTER TABLE tmp ADD COLUMN n oidvector "
                   "default '0 0 0 0'::oidvector");
  util.execute("ALTER TABLE tmp ADD COLUMN p smgr "
                   "default 'magnetic disk'::smgr");
  util.execute("ALTER TABLE tmp ADD COLUMN q point default '(0,0)'::point");
  util.execute("ALTER TABLE tmp ADD COLUMN r lseg default '(0,0),(1,1)'::lseg");
  util.execute("ALTER TABLE tmp ADD COLUMN s path default '(1,1),(1,2),(2,2)'::path");
  util.execute("ALTER TABLE tmp ADD COLUMN t box default box(circle '((0,0), 2.0)')");

  util.execute("set datestyle=ISO,DMY;"
               "ALTER TABLE tmp ADD COLUMN u tinterval "
                   "default tinterval('2/2/2013 4:05:06', '2/2/2013 5:05:06')");
  util.execute("set datestyle=ISO,DMY;"
               "ALTER TABLE tmp ADD COLUMN v timestamp "
                   "default '2/2/2013 4:05:06'::timestamp");
  util.execute("ALTER TABLE tmp ADD COLUMN w interval default '3 4:05:06'::interval");
  util.execute("ALTER TABLE tmp ADD COLUMN x float8[] default ARRAY[0, 0, 0]");
  util.execute("ALTER TABLE tmp ADD COLUMN y float4[] default ARRAY[0, 0, 0]");
  util.execute("ALTER TABLE tmp ADD COLUMN z int2[] default ARRAY[0, 0, 0]");

  util.execSQLFile("catalog/sql/alter-table-addcol-insert-alltypes.sql",
				   "catalog/ans/alter-table-addcol-insert-alltypes.ans");
  // cleanup
  util.execute("drop table tmp");
}
