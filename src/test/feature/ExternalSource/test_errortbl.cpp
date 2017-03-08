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
#include "lib/string_util.h"
#include "lib/gpfdist.h"

using hawq::test::SQLUtility;

class TestErrorTable : public ::testing::Test {
  public:
   TestErrorTable() {}
   ~TestErrorTable() {}
};

TEST_F(TestErrorTable, TestErrorTableAll) {
  
  SQLUtility util;

  hawq::test::GPfdist gpdfist(&util);

  gpdfist.init_gpfdist();
  
  // readable external table with error table
  util.execute(
      "CREATE EXTERNAL TABLE EXT_NATION1 ( N_NATIONKEY  INTEGER ,"
      "N_NAME       CHAR(25) ,"
      "N_REGIONKEY  INTEGER ,"
      "N_COMMENT    VARCHAR(152))"
      "location ('gpfdist://localhost:7070/nation_error50.tbl')"
      "FORMAT 'text' (delimiter '|')"
      "LOG ERRORS INTO EXT_NATION_ERROR1 SEGMENT REJECT LIMIT 51;");

  util.execute(
      "CREATE EXTERNAL TABLE EXT_NATION2 ( N_NATIONKEY  INTEGER ,"
      "N_NAME       CHAR(25) ,"
      "N_REGIONKEY  INTEGER ,"
      "N_COMMENT    VARCHAR(152))"
      "location ('gpfdist://localhost:7070/nation_error50.tbl')"
      "FORMAT 'text' (delimiter '|')"
      "LOG ERRORS INTO EXT_NATION_ERROR2 SEGMENT REJECT LIMIT 50;");

  util.execute(
      "CREATE EXTERNAL TABLE EXT_NATION3 ( N_NATIONKEY  INTEGER ,"
      "N_NAME       CHAR(25) ,"
      "N_REGIONKEY  INTEGER ,"
      "N_COMMENT    VARCHAR(152))"
      "location ('gpfdist://localhost:7070/nation.tbl')"
      "FORMAT 'text' (delimiter '|')"
      "LOG ERRORS INTO EXT_NATION_ERROR3 SEGMENT REJECT LIMIT 50;");

  // use existing error table
  util.execute(
      "CREATE EXTERNAL TABLE EXT_NATION_WITH_EXIST_ERROR_TABLE ( N_NATIONKEY  INTEGER ,"
      "N_NAME       CHAR(25) ,"
      "N_REGIONKEY  INTEGER ,"
      "N_COMMENT    VARCHAR(152))"
      "location ('gpfdist://localhost:7070/nation_error50.tbl')"
      "FORMAT 'text' (delimiter '|')"
      "LOG ERRORS INTO EXT_NATION_ERROR1 SEGMENT REJECT LIMIT 51;");

  util.query("select * from EXT_NATION1;", 25);
  util.query("select * from EXT_NATION_ERROR1;", 50);
  util.query("select * from EXT_NATION_WITH_EXIST_ERROR_TABLE;", 25);
  util.query("select * from EXT_NATION_ERROR1;", 100);
  util.execute("select * from EXT_NATION2;", false);
  util.query("select * from EXT_NATION_ERROR2;", 0);
  util.query("select * from EXT_NATION3;", 25);
  util.query("select * from EXT_NATION_ERROR3;", 0);
  
  util.execute("truncate EXT_NATION_ERROR1;");
  util.query("select * from EXT_NATION1 as x, EXT_NATION3 as y where x.n_nationkey = y.n_nationkey;", 25);
  util.query("select * from EXT_NATION_ERROR1;", 50);
  
  util.execute("select * from EXT_NATION1 as x, EXT_NATION1 as y where x.n_nationkey = y.n_nationkey;", false);
  util.execute("select * from EXT_NATION1 as x, EXT_NATION_WITH_EXIST_ERROR_TABLE as y where x.n_nationkey = y.n_nationkey;", false);

  util.execute(
      "CREATE WRITABLE EXTERNAL TABLE EXT_NATION_WRITABLE ( N_NATIONKEY  INTEGER , "
      "N_NAME       CHAR(25) , "
      "N_REGIONKEY  INTEGER , "
      "N_COMMENT    VARCHAR(152)) "
      "LOCATION ('gpfdist://localhost:7070/nation_error50.tbl') " 
      "FORMAT 'text' (delimiter '|') "
      "LOG ERRORS INTO EXT_NATION_ERROR_WRITABLE SEGMENT REJECT LIMIT 5;",
      false);

  util.execute("drop external table EXT_NATION_WITH_EXIST_ERROR_TABLE;");
  util.execute("drop external table EXT_NATION1;");
  util.execute("drop table EXT_NATION_ERROR1 CASCADE;");
  util.execute("drop external table EXT_NATION2;");
  util.execute("drop table EXT_NATION_ERROR2 CASCADE;");
  util.execute("drop external table EXT_NATION3;");
  util.execute("drop table EXT_NATION_ERROR3 CASCADE;");

  gpdfist.finalize_gpfdist();
}
