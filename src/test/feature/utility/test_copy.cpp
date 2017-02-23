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

class TestCopy: public ::testing::Test {
 public:
  TestCopy() {}
  ~TestCopy() {}
};

TEST_F(TestCopy, TestCOPY) {
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

  // test
  std::string path = util.getTestRootPath();
  util.execute("COPY aggtest FROM '" + path + "/utility/data/agg.data'");
  util.execute("COPY onek FROM '" + path + "/utility/data/onek.data'");
  util.execute("COPY onek TO '" + path + "/utility/ans/onek.data'");
  util.execute("TRUNCATE onek");
  util.execute("COPY onek FROM '" + path + "/utility/ans/onek.data'");
  util.execute("COPY tenk1 FROM '" + path + "/utility/data/tenk.data'");
  util.execute("COPY slow_emp4000 FROM '" + path + "/utility/data/rect.data'");
  util.execute("COPY person FROM '" + path + "/utility/data/person.data'");
  util.execute("COPY emp FROM '" + path + "/utility/data/emp.data'");
  util.execute("COPY student FROM '" + path + "/utility/data/student.data'");
  util.execute("COPY stud_emp FROM '" + path + "/utility/data/stud_emp.data'");
  util.execute("COPY road FROM '" + path + "/utility/data/streets.data'");
  util.execute("COPY real_city FROM '" + path + "/utility/data/real_city.data'");
  util.execute("COPY hash_i4_heap FROM '" + path + "/utility/data/hash.data'");
  util.execute("COPY hash_name_heap FROM '" + path + "/utility/data/hash.data'");
  util.execute("COPY hash_txt_heap FROM '" + path + "/utility/data/hash.data'");
  util.execute("COPY hash_f8_heap FROM '" + path + "/utility/data/hash.data'");

  util.execute("COPY bt_i4_heap FROM '" + path + "/utility/data/desc.data'");
  util.execute("COPY bt_name_heap FROM '" + path + "/utility/data/hash.data'");
  util.execute("COPY bt_txt_heap FROM '" + path + "/utility/data/desc.data'");
  util.execute("COPY bt_f8_heap FROM '" + path + "/utility/data/hash.data'");
  util.execute("COPY array_op_test FROM '" + path + "/utility/data/array.data'");
  util.execute("COPY array_index_op_test FROM '" + path + "/utility/data/array.data'");

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

TEST_F(TestCopy, TestCOPY2) {
  hawq::test::SQLUtility util;

  // prepare
  util.execute("DROP TABLE IF EXISTS copytest CASCADE");
  util.execute("DROP TABLE IF EXISTS copytest2 CASCADE");

  // test
  std::string path = util.getTestRootPath();
  util.execute("CREATE TABLE copytest (style text, "
                                      "test text,"
                                      "filler int)");
  util.execute("INSERT INTO copytest VALUES('DOS',E'abc\r\ndef',1)");
  util.execute("INSERT INTO copytest VALUES('Unix',E'abc\r\ndef',2)");
  util.execute("INSERT INTO copytest VALUES('Mac', E'abc\rdef',3)");
  util.execute("INSERT INTO copytest VALUES(E'esc\\ape', E'a\\r\\\r\\\n\\nb',4)");

  util.execute("COPY copytest TO '" + path + "/utility/ans/copytest.csv' CSV");
  util.execute("CREATE TABLE copytest2 (like copytest)");
  util.execute("COPY copytest2 FROM '" + path + "/utility/ans/copytest.csv' CSV");
  util.query("SELECT * FROM copytest EXCEPT SELECT * FROM copytest2", "");
  util.execute("TRUNCATE copytest2");

  util.execute("COPY copytest to '" + path + "/utility/ans/copytest.csv' CSV "
		       "QUOTE '''' ESCAPE E'\\\\\\\\'");
  util.execute("COPY copytest2 FROM '" + path + "/utility/ans/copytest.csv' CSV "
		       "QUOTE '''' ESCAPE E'\\\\\\\\'");
  util.query("SELECT * FROM copytest EXCEPT SELECT * FROM copytest2", "");

  util.execSQLFile("utility/sql/copy-stdio.sql",
		  	  	   "utility/ans/copy-stdio.ans");
  // clean up
  util.execute("DROP TABLE copytest");
  util.execute("DROP TABLE copytest2");
}


