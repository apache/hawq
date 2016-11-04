#include "gtest/gtest.h"

#include "lib/sql_util.h"
#include "lib/string_util.h"

using hawq::test::SQLUtility;

class TestErrorTable : public ::testing::Test {
  public:
   TestErrorTable() {}
   ~TestErrorTable() {}
};

TEST_F(TestErrorTable, TestErrorTableAll) {
  
  SQLUtility util;
  auto init_gpfdist = [&] () {
    auto sql = "CREATE EXTERNAL WEB TABLE gpfdist_status (x text) "
        "execute E'( python %s/bin/lib/gppinggpfdist.py localhost:7070 2>&1 || echo) ' "
        "on SEGMENT 0 "
        "FORMAT 'text' (delimiter '|');";
    auto GPHOME = getenv("GPHOME");
    util.execute(hawq::test::stringFormat(sql, GPHOME));

    sql = "CREATE EXTERNAL WEB TABLE gpfdist_start (x text) "
        "execute E'((%s/bin/gpfdist -p 7070 -d %s  </dev/null >/dev/null 2>&1 &); sleep 2; echo \"starting\"...) ' "
        "on SEGMENT 0 "
        "FORMAT 'text' (delimiter '|');";
    std::string path = util.getTestRootPath() + "/ExternalSource/data";
    util.execute(hawq::test::stringFormat(sql, GPHOME, path.c_str()));
  
    util.execute(
        "CREATE EXTERNAL WEB TABLE gpfdist_stop (x text) "
        "execute E'(/bin/pkill gpfdist || killall gpfdist) > /dev/null 2>&1; echo stopping...' "
        "on SEGMENT 0 "
        "FORMAT 'text' (delimiter '|');");
    util.execute("select * from gpfdist_stop;");
    util.execute("select * from gpfdist_status;");
    util.execute("select * from gpfdist_start;");
    util.execute("select * from gpfdist_status;");
  };

  auto finalize_gpfdist = [&] () {
    util.execute("drop external table EXT_NATION_WITH_EXIST_ERROR_TABLE;");
    util.execute("drop external table EXT_NATION1;");
    util.execute("drop table EXT_NATION_ERROR1 CASCADE;");
    util.execute("drop external table EXT_NATION2;");
    util.execute("drop table EXT_NATION_ERROR2 CASCADE;");
    util.execute("drop external table EXT_NATION3;");
    util.execute("drop table EXT_NATION_ERROR3 CASCADE;");
    util.execute("select * from gpfdist_stop;");
    util.execute("select * from gpfdist_status;");
    util.execute("drop external table gpfdist_status;");
    util.execute("drop external table gpfdist_start;");
    util.execute("drop external table gpfdist_stop;");
  };

  init_gpfdist();
  
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

  finalize_gpfdist();
}
