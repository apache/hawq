#include <string>

#include "lib/command.h"
#include "lib/sql_util.h"

#include "gtest/gtest.h"

using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;

/* This test suite may consume more than 80 seconds. */
class TestHawqRegister : public ::testing::Test {
 public:
  TestHawqRegister() {}
  ~TestHawqRegister() {}
};

TEST_F(TestHawqRegister, TestSingleHawqFile) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hawq register postgres hawqregister hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.query("select * from hawqregister;", 3);
	util.execute("insert into hawqregister values(1);");
	util.query("select * from hawqregister;", 4);
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestSingleHiveFile) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hive.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath + " hdfs://localhost:8020/hawq_register_hive.paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hawq register postgres hawqregister hdfs://localhost:8020/hawq_register_hive.paq"));

	util.query("select * from hawqregister;", 1);
	util.execute("insert into hawqregister values(1);");
	util.query("select * from hawqregister;", 2);
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestFiles) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath1 = rootPath + relativePath;
	relativePath = "/ManagementTool/test_hawq_register_hive.paq";
	string filePath2 = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -mkdir -p hdfs://localhost:8020/hawq_register_test/t/t"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath1 + " hdfs://localhost:8020/hawq_register_test/hawq1.paq"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath1 + " hdfs://localhost:8020/hawq_register_test/hawq2.paq"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath1 + " hdfs://localhost:8020/hawq_register_test/t/hawq.paq"));

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath2 + " hdfs://localhost:8020/hawq_register_test/hive1.paq"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath2 + " hdfs://localhost:8020/hawq_register_test/hive2.paq"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath2 + " hdfs://localhost:8020/hawq_register_test/t/hive.paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hawq register postgres hawqregister hdfs://localhost:8020/hawq_register_test"));

	util.query("select * from hawqregister;", 12);
	util.execute("insert into hawqregister values(1);");
	util.query("select * from hawqregister;", 13);
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm -r hdfs://localhost:8020/hawq_register_test"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestHashDistributedTable) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register postgres hawqregister hdfs://localhost:8020/hawq_register_hawq.paq"));
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm hdfs://localhost:8020/hawq_register_hawq.paq"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestNotParquetFile) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_not_paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath + " hdfs://localhost:8020/hawq_register_test_not_paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register postgres hawqregister hdfs://localhost:8020/hawq_register_test_not_paq"));
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm hdfs://localhost:8020/hawq_register_test_not_paq"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestNotParquetTable) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register postgres hawqregister hdfs://localhost:8020/hawq_register_hawq.paq"));
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm hdfs://localhost:8020/hawq_register_hawq.paq"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestFileNotExist) {
	SQLUtility util;

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register postgres hawqregister /hdfs://localhost:8020hawq_register_file_not_exist"));
	util.query("select * from hawqregister;", 0);

	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestNotHDFSPath) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register postgres hawqregister /hawq_register_hawq.paq"));
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm hdfs://localhost:8020/hawq_register_hawq.paq"));
	util.execute("drop table hawqregister;");
}
