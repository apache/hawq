#include <string>

#include "gtest/gtest.h"
#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"
#include "lib/hdfs_config.h"
#include "test_hawq_register.h"

using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;
using hawq::test::HdfsConfig;

TEST_F(TestHawqRegister, TestUsage1SingleHawqFile) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_hawq.paq hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.query("select * from hawqregister;", 3);
	util.execute("insert into hawqregister values(1);");
	util.query("select * from hawqregister;", 4);
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1SingleHiveFile) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hive.paq");
	string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_hive.paq", filePath.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_hive.paq hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.query("select * from hawqregister;", 1);
	util.execute("insert into hawqregister values(1);");
	util.query("select * from hawqregister;", 2);
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestDataTypes) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	/* This parquet file is generate by HIVE, using the table created by */
	/* 'create table parquet(a boolean, b tinyint, c smallint, d int, e bigint, f date, g float, h double, i string, j binary, k char(10), l varchar(10)) stored as parquet;' */
	string relativePath("/ManagementTool/test_hawq_register_data_types.paq");
	string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_data_types.paq", filePath.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.execute("create table hawqregister(a bool, b int2, c int2, d int4, e int8, f date, g float4, h float8, i varchar, j bytea, k char, l varchar) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_data_types.paq hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.query("select * from hawqregister;", 1);
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestAllNULL) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	/* This parquet file is generate by HIVE, using the table created by */
	/* 'create table parquet(a boolean, b tinyint, c smallint, d int, e bigint, f date, g float, h double, i string, j binary, k char(10), l varchar(10)) stored as parquet;' */
	/* with all the values set to NULL */
	string relativePath("/ManagementTool/test_hawq_register_null.paq");
	string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_data_types.paq", filePath.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.execute("create table hawqregister(a bool, b int2, c int2, d int4, e int8, f date, g float4, h float8, i varchar, j bytea, k char, l varchar) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_data_types.paq hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.query("select * from hawqregister;", 1);
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestFiles) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath1 = rootPath + relativePath;
	relativePath = "/ManagementTool/test_hawq_register_hive.paq";
	string filePath2 = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -mkdir -p %s %s/hawq_register_test/t/t", filePath1.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_test/hawq1.paq", filePath1.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_test/hawq2.paq", filePath1.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_test/t/hawq.paq", filePath1.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_test/hive1.paq", filePath2.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_test/hive2.paq", filePath2.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_test/t/hive.paq", filePath2.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_test hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.query("select * from hawqregister;", 12);
	util.execute("insert into hawqregister values(1);");
	util.query("select * from hawqregister;", 13);
    cmd = hawq::test::stringFormat("hadoop fs -rm -r %s/hawq_register_test", getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1NotParquetFile) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_not_paq");
	string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_test_not_paq", filePath.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_test_not_paq hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(1, Command::getCommandStatus(cmd));
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hadoop fs -rm %s/hawq_register_test_not_paq", getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1NotParquetTable) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_hawq.paq hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(1, Command::getCommandStatus(cmd));
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hadoop fs -rm  %s/hawq_register_hawq.paq", getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1FileNotExist) {
	SQLUtility util;

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

    auto cmd = hawq::test::stringFormat("hawq register -d %s -f %shawq_register_file_not_exist hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(1, Command::getCommandStatus(cmd));
	util.query("select * from hawqregister;", 0);

	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1NotHDFSPath) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/hawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_hawq.paq hawqregister", HAWQ_DB, getHdfsLocation().c_str());
	EXPECT_EQ(1, Command::getCommandStatus(cmd));
	util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hadoop fs -rm %s/hawq_register_hawq.paq", getHdfsLocation().c_str());
	EXPECT_EQ(0, Command::getCommandStatus(cmd));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestEmptyTable) {
  SQLUtility util;
  util.execute("drop table if exists t9;");
  util.execute("create table t9(i int) with (appendonly=true, orientation=row) distributed randomly;");
  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o t9.yml testhawqregister_testemptytable.t9"));
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c t9.yml testhawqregister_testemptytable.nt9"));
  util.query("select * from nt9;", 0);
  EXPECT_EQ(0, Command::getCommandStatus("rm -rf t9.yml"));
  util.execute("drop table t9;");
  util.execute("drop table nt9;");
}

TEST_F(TestHawqRegister, TestIncorrectYaml) {
  SQLUtility util;
  string filePath = util.getTestRootPath() + "/ManagementTool/";

  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_pagesize.yml xx"));
  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_rowgroupsize.yml xx"));
  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_filesize.yml xx"));
  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "wrong_schema.yml xx"));
  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_checksum.yml xx"));
  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "wrong_dfs_url.yml xx"));
  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_bucketnum.yml xx"));
}

TEST_F(TestHawqRegister, TestDismatchFileNumber) {
  SQLUtility util;
  string filePath = util.getTestRootPath() + "/ManagementTool/";
  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "files_incomplete.yml xx"));
}
