#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include "lib/command.h"
#include "lib/data_gen.h"
#include "lib/hawq_config.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"

#include "gtest/gtest.h"

class TestCommonLib : public ::testing::Test {
 public:
  TestCommonLib() {}
  ~TestCommonLib() {}
};

TEST_F(TestCommonLib, TestHawqConfig) {
  std::string hostname = "";
  int port = 0;

  hawq::test::HawqConfig hc;
  hc.getMaster(hostname, port);

  hostname = "";
  port = 0;
  hc.getStandbyMaster(hostname, port);

  std::vector<std::string> segmentHostname;
  std::vector<int> segmentPort;
  hc.getSlaves(segmentHostname);

  segmentHostname.clear();
  segmentPort.clear();
  hc.getTotalSegments(segmentHostname, segmentPort);

  segmentHostname.clear();
  segmentPort.clear();
  hc.getDownSegments(segmentHostname, segmentPort);

  segmentHostname.clear();
  segmentPort.clear();
  hc.getUpSegments(segmentHostname, segmentPort);

  hc.isMasterMirrorSynchronized();
  hc.isMultinodeMode();

  hc.setGucValue("default_hash_table_bucket_number", "4");
  hc.getGucValue("default_hash_table_bucket_number");
  return;
}

TEST_F(TestCommonLib, TestCommand) {
  hawq::test::Command c("ls ./");
  c.run().getResultOutput();

  hawq::test::Command::getCommandOutput("pwd");
  hawq::test::Command::getCommandStatus("date");
  hawq::test::Command::getCommandStatus("datewrong");
  hawq::test::Command::getCommandOutput("datewrong");
}

TEST_F(TestCommonLib, TestSqlUtil) {
  hawq::test::SQLUtility util;
  util.execute("create table test(p int, q float)");
  util.execute("insert into test values(1,1.1),(2,2.2)");
  util.execute("select * from no_exist_table;", false);
  util.executeIgnore("select * from no_exist_table;");
  util.query("select * from test", 2);
  util.query("select * from test", "1|1.1|\n2|2.2|\n");
  util.execSQLFile("testlib/sql/sample.sql", "testlib/ans/sample.ans");
  auto err_msg = util.execute("select * from non_exist_tbl;", false);
  EXPECT_EQ(err_msg,
            "ERROR:  relation \"non_exist_tbl\" does not exist\n"
            "LINE 1: ...ARCH_PATH=TestCommonLib_TestSqlUtil;select * from non_exist_...\n"
            "                                                             ^\n");
  err_msg = util.execute("drop table non_exist_tbl;", false);
  EXPECT_EQ(err_msg, "ERROR:  table \"non_exist_tbl\" does not exist\n");
}

TEST_F(TestCommonLib, TestStringFormat) {
  auto s1 = hawq::test::stringFormat("%s are welcome to apache %s project", "you", "HAWQ");
  EXPECT_EQ(s1, "you are welcome to apache HAWQ project");
}

TEST_F(TestCommonLib, TestDataGenerator) {
  hawq::test::SQLUtility util;
  hawq::test::DataGenerator dGen(&util);
  dGen.genSimpleTable("simpleAO");
  dGen.genSimpleTable("simpleParquet", true, "parquet");

  dGen.genTableWithFullTypes("fullTypeAO");
  dGen.genTableWithSeries("tSeries");

  dGen.genTableWithNull("tNull");
}
