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
  util.query("select avg(b) as bavg from t group by a order by bavg","3|\n15|\n62|\n");
  util.query("select newavg(b) as bavg from t group by a order by bavg","3|\n15|\n62|\n");
}


