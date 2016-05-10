#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include "lib/command.h"
#include "lib/common.h"
#include "lib/hawq-config.h"

#include "gtest/gtest.h"

class TestCommonLib: public ::testing::Test {
  public:
    TestCommonLib() {
    }
    ~TestCommonLib() {
    }
};

TEST_F(TestCommonLib, TestHawqConfig) {
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

  PSQL psql("postgres", "localhost", "5432", "huanzhang");
  psql.setOutputFile("/tmp/testlog");
  psql.runSQLCommand("create table foo(i int)");
  psql.runSQLCommand("SELECT * from foo LIMIT 1");

  psql.setOutputFile("/tmp/testlog2");
  psql.runSQLFile("/tmp/test.sql");

  psql.checkDiff("/tmp/testlog", "/tmp/testlog");

  PSQLQueryResult res = psql.getQueryResult("SELECT * from foo LIMIT 3");

  const std::vector<std::vector<std::string> > rows = res.getRows();
  const std::vector<std::string> fields = res.getFields();

  psql.runSQLCommand("drop table foo");
}
