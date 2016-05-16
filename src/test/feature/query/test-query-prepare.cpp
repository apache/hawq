#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include "lib/command.h"
#include "lib/common.h"
#include "lib/data-gen.h"
#include "lib/hawq-config.h"
#include "lib/sql-util.h"

#include "gtest/gtest.h"

class TestQueryPrepare : public ::testing::Test {
 public:
  TestQueryPrepare() {}
  ~TestQueryPrepare() {}
};

TEST_F(TestQueryPrepare, TestPrepareEnvironment) {
  SQLUtility util;
  util.execSQLFile("query/sql/prepare_prepenv.sql",
		  	  	   "query/ans/prepare_prepenv.ans");
}

TEST_F(TestQueryPrepare, TestPrepareUniqueness) {

}

TEST_F(TestQueryPrepare, TestPrepareArgList) {

}

TEST_F(TestQueryPrepare, TestPrepareCleanup) {

}

TEST_F(TestQueryPrepare, TestHawqConfig) {
  std::string hostname = "";
  int port = 0;
  struct passwd *pw;
  uid_t uid;
  uid = geteuid();
  pw = getpwuid(uid);
  std::string uname(pw->pw_name);
  HawqConfig hc(uname);
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
  Command c("ls ./");
  c.run().getResultOutput();

  Command::getCommandOutput("pwd");
  Command::getCommandStatus("date");
  Command::getCommandStatus("datewrong");
  Command::getCommandOutput("datewrong");
}

TEST_F(TestCommonLib, TestSqlUtil) {
  SQLUtility util;
  util.execute("create table test(p int, q float)");
  util.execute("insert into test values(1,1.1),(2,2.2)");
  util.query("select * from test", 2);
  util.query("select * from test", "1|1.1|\n2|2.2|\n");
  util.execSQLFile("testlib/sql/sample.sql", "testlib/ans/sample.ans");
}

TEST_F(TestCommonLib, TestDataGenerator) {
  SQLUtility util;
  DataGenerator dGen(&util);
  dGen.genSimpleTable("simpleAO");
  dGen.genSimpleTable("simpleParquet", true, "parquet");

  dGen.genTableWithFullTypes("fullTypeAO");
  dGen.genTableWithSeries("tSeries");

  dGen.genTableWithNull("tNull");
}
