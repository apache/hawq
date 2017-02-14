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
#include <algorithm>

#include "lib/sql_util.h"

#include "gtest/gtest.h"

class TestCreateTable : public ::testing::Test {
 public:
  TestCreateTable() {}
  ~TestCreateTable() {}
  std::string getNsOid(hawq::test::SQLUtility &util);
};

std::string TestCreateTable::getNsOid(hawq::test::SQLUtility &util) {
  std::string schema = util.getSchemaName();
  std::transform(schema.begin(), schema.end(), schema.begin(), ::tolower);
  const hawq::test::PSQLQueryResult& result = util.executeQuery("SELECT oid FROM pg_namespace WHERE nspname='"+schema+"'");
  EXPECT_EQ(result.rowCount(), 1);
  std::string nsOid = result.getRows()[0][0];
  return nsOid; 
}

TEST_F(TestCreateTable, TestCreateTable1) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS aggtest CASCADE");
  util.execute("DROP TABLE IF EXISTS tenk1 CASCADE");
  util.execute("DROP TABLE IF EXISTS slow_emp4000 CASCADE");
  util.execute("DROP TABLE IF EXISTS person CASCADE");
  util.execute("DROP TABLE IF EXISTS onek CASCADE");
  util.execute("DROP TABLE IF EXISTS emp CASCADE");
  util.execute("DROP TABLE IF EXISTS student CASCADE");
  util.execute("DROP TABLE IF EXISTS stud_emp CASCADE");
  util.execute("DROP TABLE IF EXISTS real_city CASCADE");
  util.execute("DROP TABLE IF EXISTS road CASCADE");
  util.execute("DROP TABLE IF EXISTS hash_i4_heap CASCADE");
  util.execute("DROP TABLE IF EXISTS hash_name_heap CASCADE");
  util.execute("DROP TABLE IF EXISTS hash_txt_heap CASCADE");
  util.execute("DROP TABLE IF EXISTS hash_f8_heap CASCADE");
  util.execute("DROP TABLE IF EXISTS bt_i4_heap CASCADE");
  util.execute("DROP TABLE IF EXISTS bt_name_heap CASCADE");
  util.execute("DROP TABLE IF EXISTS bt_txt_heap CASCADE");
  util.execute("DROP TABLE IF EXISTS bt_f8_heap CASCADE");
  util.execute("DROP TABLE IF EXISTS array_op_test CASCADE");
  util.execute("DROP TABLE IF EXISTS array_index_op_test CASCADE");

  // test
  util.execute("CREATE TABLE aggtest (a int2, b float4)");

  util.execute("CREATE TABLE tenk1 (unique1     int4,"
    							   "unique2     int4,"
    							   "two         int4,"
    							   "four        int4,"
    							   "ten         int4,"
    							   "twenty      int4,"
    							   "hundred     int4,"
    							   "thousand    int4,"
    							   "twothousand int4,"
    							   "fivethous   int4,"
    							   "tenthous    int4,"
    							   "odd         int4,"
    							   "even        int4,"
    							   "stringu1    name,"
    							   "stringu2    name,"
    							   "string4     name) WITH OIDS");

  util.execute("CREATE TABLE slow_emp4000 (home_base  box)");

  util.execute("CREATE TABLE person (name        text,"
		                            "age         int4,"
		                            "location    point)");

  util.execute("CREATE TABLE onek (unique1     int4,"
                                  "unique2     int4,"
                                  "two         int4,"
                                  "four        int4,"
                                  "ten         int4,"
                                  "twenty      int4,"
                                  "hundred     int4,"
                                  "thousand    int4,"
                                  "twothousand int4,"
                                  "fivethous   int4,"
                                  "tenthous    int4,"
                                  "odd         int4,"
                                  "even        int4,"
                                  "stringu1    name,"
                                  "stringu2    name,"
                                  "string4     name)");

  util.execute("CREATE TABLE emp (salary      int4,"
                                 "manager     name)"
                                 " INHERITS (person) WITH OIDS");

  util.execute("CREATE TABLE student (gpa float8) INHERITS (person)");

  util.execute("CREATE TABLE stud_emp (percent int4) INHERITS (emp, student)");

  util.execute("CREATE TABLE real_city (pop         int4,"
                                       "cname       text,"
                                       "outline     path)");

  util.execute("CREATE TABLE road (name        text,"
		                          "thepath     path)");


  util.execute("CREATE TABLE hash_i4_heap (seqno       int4,"
                                          "random      int4)");

  util.execute("CREATE TABLE hash_name_heap (seqno       int4,"
                                            "random      name)");

  util.execute("CREATE TABLE hash_txt_heap (seqno       int4,"
                                           "random      text)");

  util.execute("CREATE TABLE hash_f8_heap (seqno       int4,"
                                          "random      float8)");

  util.execute("CREATE TABLE bt_i4_heap (seqno       int4,"
                                        "random      int4)");

  util.execute("CREATE TABLE bt_name_heap (seqno       name,"
                                          "random      int4)");

  util.execute("CREATE TABLE bt_txt_heap (seqno       text,"
                                         "random      int4)");

  util.execute("CREATE TABLE bt_f8_heap (seqno       float8,"
                                        "random      int4)");

  util.execute("CREATE TABLE array_op_test (seqno       int4,"
                                           "i           int4[],"
                                           "t           text[])");

  util.execute("CREATE TABLE array_index_op_test (seqno       int4,"
                                                 "i           int4[],"
                                                 "t           text[])");

  // cleanup
  util.execute("DROP TABLE array_index_op_test");
  util.execute("DROP TABLE array_op_test");
  util.execute("DROP TABLE bt_f8_heap");
  util.execute("DROP TABLE bt_txt_heap");
  util.execute("DROP TABLE bt_name_heap");
  util.execute("DROP TABLE bt_i4_heap");
  util.execute("DROP TABLE hash_f8_heap");
  util.execute("DROP TABLE hash_txt_heap");
  util.execute("DROP TABLE hash_name_heap");
  util.execute("DROP TABLE hash_i4_heap");
  util.execute("DROP TABLE road");
  util.execute("DROP TABLE real_city");
  util.execute("DROP TABLE stud_emp");
  util.execute("DROP TABLE student");
  util.execute("DROP TABLE emp");
  util.execute("DROP TABLE onek");
  util.execute("DROP TABLE person");
  util.execute("DROP TABLE slow_emp4000");
  util.execute("DROP TABLE tenk1");
  util.execute("DROP TABLE aggtest");
}

TEST_F(TestCreateTable, TestCreateTableInherits) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS t1_1_6, t1_1_5, t1_1_4, t1_1_3, "
		                            "t1_1_2, t1_1_1, t1_1_w, t1_1, t1 CASCADE");

  // test
  util.execute("CREATE TABLE t1(c1 int)");
  util.execute("CREATE TABLE t1_1(c2 int) INHERITS(t1)");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t1_1_w(c2 int) INHERITS(t1) WITH (bucketnum=3)",
      "NOTICE:  Table has parent, setting distribution columns to match parent table\n"
      "ERROR:  distribution policy for \"t1_1_w\" must be the same as that for \"t1\"");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t1_1_1(c2 int) INHERITS (t1) DISTRIBUTED BY (c1)",
      "ERROR:  distribution policy for \"t1_1_1\" must be the same as that for \"t1\"");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t1_1_2(c2 int) INHERITS (t1) DISTRIBUTED BY (c1)",
      "ERROR:  distribution policy for \"t1_1_2\" must be the same as that for \"t1\"");
  util.execute("CREATE TABLE t1_1_3(c2 int) INHERITS (t1) DISTRIBUTED RANDOMLY");

  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t1_1_4(c2 int) INHERITS (t1) WITH (bucketnum = 3) DISTRIBUTED BY(c1)",
      "ERROR:  distribution policy for \"t1_1_4\" must be the same as that for \"t1\"");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t1_1_5(c2 int) INHERITS (t1) WITH (bucketnum = 5) DISTRIBUTED BY(c2)",
      "ERROR:  distribution policy for \"t1_1_5\" must be the same as that for \"t1\"");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t1_1_6(c2 int) INHERITS (t1) WITH (bucketnum = 7) DISTRIBUTED RANDOMLY",
      "ERROR:  distribution policy for \"t1_1_6\" must be the same as that for \"t1\"");

  std::string nsOid = getNsOid(util);

  util.execute("CREATE TABLE t1_1_w(c2 int) INHERITS(t1) WITH (bucketnum=6)");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_1' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_1_w' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_1_3' AND relnamespace="+nsOid+")",
		     "6||\n");

  util.execute("DROP TABLE t1_1_3, t1_1_w, t1_1, t1");
}

TEST_F(TestCreateTable, TestCreateTableDistribution1) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS t1_3_4, t1_3_3, t1_3_2, t1_3_1, t1_3_w, t1_3 CASCADE");
  util.execute("DROP TABLE IF EXISTS t1_2_4, t1_2_3, t1_2_2, t1_2_1, t1_2_w, t1_2 CASCADE");
  util.execute("DROP TABLE IF EXISTS t1 CASCADE");
  util.execute("CREATE TABLE t1(c1 int)");

  // test
  util.execute("CREATE TABLE t1_2 (LIKE t1)");
  util.execute("CREATE TABLE t1_2_w(LIKE t1) WITH (bucketnum = 4)");
  util.execute("CREATE TABLE t1_2_1(LIKE t1) DISTRIBUTED BY (c1)");
  util.execute("CREATE TABLE t1_2_2(LIKE t1) DISTRIBUTED RANDOMLY");
  util.execute("CREATE TABLE t1_2_3(LIKE t1) WITH (bucketnum = 4) DISTRIBUTED BY (c1)");
  util.execute("CREATE TABLE t1_2_4(LIKE t1) WITH (bucketnum = 4) DISTRIBUTED RANDOMLY");

  std::string nsOid = getNsOid(util);
  
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_2' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_2_w' AND relnamespace="+nsOid+")",
		     "4||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_2_1' AND relnamespace="+nsOid+")",
		     "6|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_2_2' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_2_3' AND relnamespace="+nsOid+")",
		     "4|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_2_4' AND relnamespace="+nsOid+")",
		     "4||\n");

  util.execute("CREATE TABLE t1_3 AS (SELECT * FROM t1)");
  util.execute("CREATE TABLE t1_3_w WITH (bucketnum = 4) AS (SELECT * FROM t1)");
  util.execute("CREATE TABLE t1_3_1 AS (SELECT * FROM  t1) DISTRIBUTED BY (c1)");
  util.execute("CREATE TABLE t1_3_2 AS (SELECT * FROM  t1) DISTRIBUTED RANDOMLY");
  util.execute("CREATE TABLE t1_3_3 WITH (bucketnum = 6) AS (SELECT * FROM  t1) DISTRIBUTED BY (c1)");
  util.execute("CREATE TABLE t1_3_4 WITH (bucketnum = 7) AS (SELECT * FROM  t1) DISTRIBUTED RANDOMLY");

  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_2' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_3_w' AND relnamespace="+nsOid+")",
		     "4||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_3_1' AND relnamespace="+nsOid+")",
		     "6|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_3_2' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_3_3' AND relnamespace="+nsOid+")",
		     "6|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname='t1_3_4' AND relnamespace="+nsOid+")",
		     "7||\n");

  // cleanup
  util.execute("DROP TABLE t1_3_4, t1_3_3, t1_3_2, t1_3_1, t1_3_w, t1_3");
  util.execute("DROP TABLE t1_2_4, t1_2_3, t1_2_2, t1_2_1, t1_2_w, t1_2");
  util.execute("DROP TABLE t1 CASCADE");
}

TEST_F(TestCreateTable, TestCreateTableDistribution2) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS t2_2, t2_2_w, t2_2_1, t2_2_2, t2_2_3, t2_2_4 CASCADE");
  util.execute("DROP TABLE IF EXISTS t2_3, t2_3_w, t2_3_1, t2_3_2, t2_3_3, t2_3_4 CASCADE");
  util.execute("DROP TABLE IF EXISTS t2_1_1, t2_1, t2 CASCADE");
  util.execute("CREATE TABLE t2(c1 int) DISTRIBUTED BY (c1)");

  // test
  util.execute("CREATE TABLE t2_1(c2 int) INHERITS (t2)");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t2_1_w(c2 int) INHERITS (t2) WITH (bucketnum = 3)",
      "NOTICE:  Table has parent, setting distribution columns to match parent table\n"
      "ERROR:  distribution policy for \"t2_1_w\" must be the same as that for \"t2\"");
  util.execute("CREATE TABLE t2_1_1(c2 int) INHERITS (t2) DISTRIBUTED BY (c1)");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t2_1_2(c2 int) INHERITS (t2) DISTRIBUTED BY (c2)",
      "ERROR:  distribution policy for \"t2_1_2\" must be the same as that for \"t2\"");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t2_1_3(c2 int) INHERITS (t2) DISTRIBUTED RANDOMLY",
      "ERROR:  distribution policy for \"t2_1_3\" must be the same as that for \"t2\"");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t2_1_4(c2 int) INHERITS (t2) WITH (bucketnum = 3) DISTRIBUTED BY (c1)",
      "ERROR:  distribution policy for \"t2_1_4\" must be the same as that for \"t2\"");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t2_1_5(c2 int) INHERITS (t2) WITH (bucketnum = 5) DISTRIBUTED BY (c2)",
      "ERROR:  distribution policy for \"t2_1_5\" must be the same as that for \"t2\"");
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t2_1_6(c2 int) INHERITS (t2) WITH (bucketnum = 7) DISTRIBUTED RANDOMLY",
      "ERROR:  distribution policy for \"t2_1_6\" must be the same as that for \"t2\"");

  std::string nsOid = getNsOid(util);
  
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1_1' AND relnamespace="+nsOid+")",
		     "6|{1}|\n");

  util.execute("CREATE TABLE t2_2(LIKE t2)");
  util.execute("CREATE TABLE t2_2_w(LIKE t2) WITH (bucketnum = 4)");
  util.execute("CREATE TABLE t2_2_1(LIKE t2) DISTRIBUTED BY (c1)");
  util.execute("CREATE TABLE t2_2_2(LIKE t2) DISTRIBUTED RANDOMLY");
  util.execute("CREATE TABLE t2_2_3(LIKE t2) WITH (bucketnum = 5) DISTRIBUTED BY (c1)");
  util.execute("CREATE TABLE t2_2_4(LIKE t2) WITH (bucketnum = 6) DISTRIBUTED RANDOMLY");

  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2' AND relnamespace="+nsOid+")",
		     "6|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_w' AND relnamespace="+nsOid+")",
		     "4|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_1' AND relnamespace="+nsOid+")",
		     "6|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_2' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_3' AND relnamespace="+nsOid+")",
		     "5|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_4' AND relnamespace="+nsOid+")",
		     "6||\n");

  util.execute("CREATE TABLE t2_3 AS (SELECT * FROM  t2)");
  util.execute("CREATE TABLE t2_3_w WITH (bucketnum = 4) AS (SELECT * FROM  t2)");
  util.execute("CREATE TABLE t2_3_1 AS (SELECT * FROM  t2) DISTRIBUTED BY (c1)");
  util.execute("CREATE TABLE t2_3_2 AS (SELECT * FROM  t2) DISTRIBUTED RANDOMLY");
  util.execute("CREATE TABLE t2_3_3 WITH (bucketnum = 5) AS (SELECT * FROM  t2) DISTRIBUTED BY (c1)");
  util.execute("CREATE TABLE t2_3_4 WITH (bucketnum = 6) AS (SELECT * FROM  t2) DISTRIBUTED RANDOMLY");

  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_w' AND relnamespace="+nsOid+")",
		     "4||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_1' AND relnamespace="+nsOid+")",
		     "6|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_2' AND relnamespace="+nsOid+")",
		     "6||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_3' AND relnamespace="+nsOid+")",
		     "5|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_4' AND relnamespace="+nsOid+")",
		     "6||\n");

  // cleanup
  util.execute("DROP TABLE t2_2, t2_2_w, t2_2_1, t2_2_2, t2_2_3, t2_2_4");
  util.execute("DROP TABLE t2_3, t2_3_w, t2_3_1, t2_3_2, t2_3_3, t2_3_4");
  util.execute("DROP TABLE t2_1_1,t2_1, t2");
}

TEST_F(TestCreateTable, TestCreateTableDistribution3) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS t3_2, t3_1, t3 CASCADE");

  // test
  util.execute("CREATE TABLE t3 (c1 int) WITH (bucketnum = 4)");
  util.execute("CREATE TABLE t3_1 (c1 int) WITH (bucketnum = 5) DISTRIBUTED BY(c1)");
  util.execute("CREATE TABLE t3_2 (c1 int) WITH (bucketnum = 6) DISTRIBUTED RANDOMLY");

  std::string nsOid = getNsOid(util);
  
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't3' AND relnamespace="+nsOid+")",
		     "4||\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't3_1' AND relnamespace="+nsOid+")",
		     "5|{1}|\n");
  util.query("SELECT bucketnum, attrnums FROM gp_distribution_policy "
		     "WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't3_2' AND relnamespace="+nsOid+")",
		     "6||\n");

  // cleanup
  util.execute("DROP TABLE t3_2, t3_1, t3");
}

TEST_F(TestCreateTable, TestCreateTableDistribution4) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS t4");

  // test
  util.executeExpectErrorMsgStartWith(
      "CREATE TABLE t4 (id int, date date, amt decimal(10,2)) "
      "DISTRIBUTED RANDOMLY PARTITION BY RANGE (date) "
      "( PARTITION Jan08 START (date '2008-01-01') INCLUSIVE WITH (bucketnum = 9),"
      "  PARTITION Feb08 START (date '2008-02-01') INCLUSIVE END (date '2008-03-01') EXCLUSIVE WITH (bucketnum = 6))",
      "ERROR:  distribution policy for \"t4_1_prt_jan08\" must be the same as that for \"t4\"");
  util.execute(
      "CREATE TABLE t4 (id int, date date, amt decimal(10,2)) "
      "DISTRIBUTED RANDOMLY PARTITION BY RANGE (date) "
      "( PARTITION Jan08 START (date '2008-01-01') INCLUSIVE WITH (bucketnum = 6),"
      "  PARTITION Feb08 START (date '2008-02-01') INCLUSIVE END (date '2008-03-01') EXCLUSIVE WITH (bucketnum = 6))");
  util.query("select bucketnum, attrnums from gp_distribution_policy where localoid='t4'::regclass",
             "6||\n");

  util.executeExpectErrorMsgStartWith(
      "ALTER TABLE t4 ADD PARTITION "
      "START (date '2008-03-01') INCLUSIVE END (date '2008-04-01') EXCLUSIVE WITH (bucketnum = 8, tablename='t4_new_part')",
      "ERROR:  distribution policy for partition must be the same as that for relation \"t4\"");
  util.execute("ALTER TABLE t4 ADD PARTITION "
               "START (date '2008-03-01') INCLUSIVE END (date '2008-04-01') EXCLUSIVE WITH (bucketnum = 6, tablename='t4_new_part')");
  util.query("select bucketnum, attrnums from gp_distribution_policy where localoid='t4_new_part'::regclass",
             "6||\n");

  // cleanup
  util.execute("DROP TABLE t4");
}



