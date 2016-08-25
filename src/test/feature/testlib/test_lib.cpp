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
#include "lib/hdfs_config.h"
#include "lib/yarn_config.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"
#include "lib/file_replace.h"

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

TEST_F(TestCommonLib, TestHdfsConfig) {
  hawq::test::HdfsConfig hc;
  hc.isHA();
  hc.isConfigKerberos();
  hc.isTruncate();
  std::string hadoopHome = hc.getHadoopHome();
  /* grant privilege for $HADOOP_HOME/etc/hadoop/hdfs-site.xml */
  std::string confPath = hadoopHome;
  confPath.append("/etc/hadoop/hdfs-site.xml");
  std::string cmd = "/usr/bin/sudo -Eu root env \"PATH=$PATH\" chmod 777 ";
  cmd.append(confPath);
  hawq::test::Command c(cmd);
  std::string result = c.run().getResultOutput();

  std::string hostname = "";
  int port = 0;
  hc.getActiveNamenode(hostname, port);

  hostname = "";
  port = 0;
  hc.getStandbyNamenode(hostname, port);

  std::vector<std::string> hostList;
  std::vector<int> portList;
  hc.getNamenodes(hostList, portList);

  hostList.clear();
  portList.clear();
  hc.getDatanodelist(hostList, portList);

  hostList.clear();
  portList.clear();
  hc.getActiveDatanodes(hostList, portList);

  hc.isSafemode();

  std::string defaultValue = hc.getParameterValue("dfs.replication");
  hc.setParameterValue("dfs.replication", "1");
  hc.getParameterValue("dfs.replication");
  hc.setParameterValue("dfs.replication", defaultValue);
}

TEST_F(TestCommonLib, TestYarnConfig) {
  hawq::test::YarnConfig hc;
  if (hc.isConfigYarn() == 0) {
    return;
  }
  hc.isHA();
  hc.isConfigKerberos();
  std::string hadoopHome = hc.getHadoopHome();
  /* grant privilege for $HADOOP_HOME/etc/hadoop/yarn-site.xml */
  std::string confPath = hadoopHome;
  confPath.append("/etc/hadoop/yarn-site.xml");
  std::string cmd = "/usr/bin/sudo -Eu root env \"PATH=$PATH\" chmod 777 ";
  cmd.append(confPath);
  hawq::test::Command c(cmd);
  std::string result = c.run().getResultOutput();

  std::string hostname = "";
  int port = 0;
  hc.getActiveRM(hostname, port);

  hostname = "";
  port = 0;
  hc.getStandbyRM(hostname, port);

  std::vector<std::string> hostList;
  std::vector<int> portList;
  hc.getRMList(hostList, portList);

  hostList.clear();
  portList.clear();
  hc.getNodeManagers(hostList, portList);

  hostList.clear();
  portList.clear();
  hc.getActiveNodeManagers(hostList, portList);

  std::string defaultValue =hc.getParameterValue("yarn.scheduler.minimum-allocation-mb");
  hc.setParameterValue("yarn.scheduler.minimum-allocation-mb", "1024");
  defaultValue =hc.getParameterValue("yarn.scheduler.minimum-allocation-mb");
  hc.setParameterValue("yarn.scheduler.minimum-allocation-mb", defaultValue);
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

TEST_F(TestCommonLib, TestFileReplace) {
  // prepare file names
  hawq::test::SQLUtility util;
  std::string d_feature_test_root(util.getTestRootPath());
  std::string f_sql_tpl(d_feature_test_root + "/testlib/sql/template.sql.source");
  std::string f_ans_tpl(d_feature_test_root + "/testlib/ans/template.ans.source");
  std::string f_sql(d_feature_test_root + "/testlib/sql/template.sql");
  std::string f_ans(d_feature_test_root + "/testlib/ans/template.ans");

  // preprocess source files to get sql/ans files
  hawq::test::FileReplace frep;
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst.insert(std::make_pair("@ABS_FEATURE_TEST_ROOT@", d_feature_test_root));

  frep.replace(f_sql_tpl, f_sql, strs_src_dst);
  frep.replace(f_ans_tpl, f_ans, strs_src_dst);

  // run sql file to get ans file and then diff it with out file
  util.execSQLFile("testlib/sql/template.sql",
                   "testlib/ans/template.ans");
}
