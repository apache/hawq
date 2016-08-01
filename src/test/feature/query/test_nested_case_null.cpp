#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <iostream>

#include "lib/command.h"
#include "lib/data_gen.h"
#include "lib/hawq_config.h"
#include "lib/sql_util.h"

#include "gtest/gtest.h"

class TestQueryNestedCaseNull : public ::testing::Test {
 public:
  TestQueryNestedCaseNull() {}
  ~TestQueryNestedCaseNull() {}
};

TEST_F(TestQueryNestedCaseNull, Test1) {
  hawq::test::SQLUtility util;

  // prepare
  util.execute("DROP TABLE IF EXISTS t CASCADE");

  // test
  util.execute("CREATE TABLE t(pid INT, wid INT, state CHARACTER VARYING(30))");
  util.execute("INSERT INTO t VALUES(1,1)");
  util.query("SELECT DECODE(DECODE(state, '', NULL, state), '-', NULL, state) AS state FROM t",
             "|\n");

  // cleanup
  util.execute("DROP TABLE t");
}
