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

class TestAggregate : public ::testing::Test {
 public:
  TestAggregate() {}
  ~TestAggregate() {}
};

TEST_F(TestAggregate, TestCreateAggregate) {
  hawq::test::SQLUtility util;

  // create a basic aggregate
  util.execute("CREATE AGGREGATE newavg (sfunc = int8_avg_accum,"
      " basetype = int8, stype = bytea, finalfunc = int8_avg,"
      " initcond1 = '{0}');", true);

  // create a full featured aggregate including schema.
  util.execute("create schema testaggregateschema;");
  util.execute("CREATE AGGREGATE testaggregateschema.newavg ("
      "sfunc = int8_avg_accum, basetype = int8, stype = bytea,"
      " finalfunc = int8_avg, initcond1 = '{0}');", true);
  util.execute("drop schema testaggregateschema cascade;");

  // multi argument aggregate
  util.execute("create function sum3(int8,int8,int8) returns int8 as "
      "'select \\$1 + \\$2 + \\$3' language sql strict immutable;",true);
  util.execute("create ordered aggregate sum2(int8,int8) ( sfunc = sum3, "
      "stype = int8, initcond = '0');", true);

  // test basic aggregate
  util.execute("drop table if exists t");
  hawq::test::DataGenerator dGen(&util);
  dGen.genSimpleTable("t");
  util.query("select sum2(a,b) as absum from t group by a,b order by absum","4|\n29|\n113|\n");
  util.query("select newavg(b) as bavg from t group by a order by bavg","3|\n15|\n62|\n");
}

TEST_F(TestAggregate, TestAggregateWithGroupingsets) {
  hawq::test::SQLUtility util;
  util.execute("drop table if exists t");
  hawq::test::DataGenerator dGen(&util);
  dGen.genAggregateTable("t");

  util.query("SELECT a, b, sum(c) sum_c FROM (SELECT a, b, c FROM t F1 "
      "LIMIT 3) F2 GROUP BY GROUPING SETS((a, b), (b)) ORDER BY a, b,"
      " sum_c;", "1|aa|10|\n1|bb|20|\n2|cc|20|\n|aa|10|\n|bb|20|\n|cc|20|\n");
}

TEST_F(TestAggregate, TestAggregateWithNull) {
  hawq::test::SQLUtility util;
  util.execute("drop table if exists t");
  hawq::test::DataGenerator dGen(&util);
  dGen.genTableWithNull("t");

  util.query(
      "select SUM(CASE WHEN a = 15 THEN 1 ELSE 0 END) as aa, b ,c from t group by b, c "
      "order by aa, b, c",
      "0|51||\n0||WET|\n1||aa|\n");
}

TEST_F(TestAggregate, TestAggregateDerivedWin) {
  hawq::test::SQLUtility util;
  util.execSQLFile("query/sql/agg-derived-win.sql",
                   "query/ans/agg-derived-win.ans");
}
