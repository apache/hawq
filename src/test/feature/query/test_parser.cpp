#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include "lib/sql_util.h"
#include "gtest/gtest.h"

class TestParser : public ::testing::Test {
 public:
  TestParser() {}
  ~TestParser() {}
};


TEST_F(TestParser, TestParserCaseGroupBy) {
  hawq::test::SQLUtility util;
  // prepare
  util.execute("DROP TABLE IF EXISTS mytable CASCADE");
  util.execute("CREATE TABLE mytable (a int, b int, c varchar(1))");
  // test
  util.execSQLFile("query/sql/parser-casegroupby.sql",
		  	  	   "query/ans/parser-casegroupby.ans");
  util.execute("DROP TABLE mytable CASCADE");
  util.execute("DROP FUNCTION negate(int)");
}

