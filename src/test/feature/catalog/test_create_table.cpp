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

class TestCreateTable : public ::testing::Test {
 public:
  TestCreateTable() {}
  ~TestCreateTable() {}
};


TEST_F(TestCreateTable, TestCreateTable1) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS aggtest");
  util.execute("DROP TABLE IF EXISTS tenk1");
  util.execute("DROP TABLE IF EXISTS slow_emp4000");
  util.execute("DROP TABLE IF EXISTS person");
  util.execute("DROP TABLE IF EXISTS onek");
  util.execute("DROP TABLE IF EXISTS emp");
  util.execute("DROP TABLE IF EXISTS student");
  util.execute("DROP TABLE IF EXISTS stud_emp");
  util.execute("DROP TABLE IF EXISTS real_city");
  util.execute("DROP TABLE IF EXISTS road");
  util.execute("DROP TABLE IF EXISTS hash_i4_heap");
  util.execute("DROP TABLE IF EXISTS hash_name_heap");
  util.execute("DROP TABLE IF EXISTS hash_txt_heap");
  util.execute("DROP TABLE IF EXISTS hash_f8_heap");
  util.execute("DROP TABLE IF EXISTS bt_i4_heap");
  util.execute("DROP TABLE IF EXISTS bt_name_heap");
  util.execute("DROP TABLE IF EXISTS bt_txt_heap");
  util.execute("DROP TABLE IF EXISTS bt_f8_heap");
  util.execute("DROP TABLE IF EXISTS array_op_test");
  util.execute("DROP TABLE IF EXISTS array_index_op_test");

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

