#include <string>

#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"

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

TEST_F(TestHawqRegister, TestUsage1SingleHawqFile) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_hawq.paq hawqregister"));

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

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_hive.paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_hive.paq hawqregister"));

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

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_data_types.paq"));

	util.execute("create table hawqregister(a bool, b int2, c int2, d int4, e int8, f date, g float4, h float8, i varchar, j bytea, k char, l varchar) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_data_types.paq hawqregister"));

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

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_data_types.paq"));

	util.execute("create table hawqregister(a bool, b int2, c int2, d int4, e int8, f date, g float4, h float8, i varchar, j bytea, k char, l varchar) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_data_types.paq hawqregister"));

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

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -mkdir -p hdfs://localhost:8020/hawq_register_test/t/t"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath1 + " hdfs://localhost:8020/hawq_register_test/hawq1.paq"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath1 + " hdfs://localhost:8020/hawq_register_test/hawq2.paq"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath1 + " hdfs://localhost:8020/hawq_register_test/t/hawq.paq"));

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath2 + " hdfs://localhost:8020/hawq_register_test/hive1.paq"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath2 + " hdfs://localhost:8020/hawq_register_test/hive2.paq"));
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath2 + " hdfs://localhost:8020/hawq_register_test/t/hive.paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_test hawqregister"));

	util.query("select * from hawqregister;", 12);
	util.execute("insert into hawqregister values(1);");
	util.query("select * from hawqregister;", 13);
	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm -r hdfs://localhost:8020/hawq_register_test"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1HashDistributedTable) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_hawq.paq hawqregister"));
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm hdfs://localhost:8020/hawq_register_hawq.paq"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1NotParquetFile) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_not_paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_test_not_paq"));

	util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_test_not_paq hawqregister"));
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm hdfs://localhost:8020/hawq_register_test_not_paq"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1NotParquetTable) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_hawq.paq hawqregister"));
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm hdfs://localhost:8020/hawq_register_hawq.paq"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1FileNotExist) {
	SQLUtility util;

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f /hdfs://localhost:8020hawq_register_file_not_exist hawqregister"));
	util.query("select * from hawqregister;", 0);

	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1NotHDFSPath) {
	SQLUtility util;
	string rootPath(util.getTestRootPath());
	string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
	string filePath = rootPath + relativePath;

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));

	util.execute("create table hawqregister(i int);");
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f /hawq_register_hawq.paq hawqregister"));
	util.query("select * from hawqregister;", 0);

	EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -rm hdfs://localhost:8020/hawq_register_hawq.paq"));
	util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1ParquetRandomly) {
  SQLUtility util;
  string rootPath(util.getTestRootPath());
  string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
  string filePath = rootPath + relativePath;
  EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));
  util.execute("drop table if exists nt;");
  util.execute("create table nt(i int) with (appendonly=true, orientation=parquet);");
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_hawq.paq nt"));
	util.query("select * from nt;", 3);
	util.execute("insert into nt values(1);");
	util.query("select * from nt;", 4);
  util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage1ParquetRandomly2) {
  SQLUtility util;
  string rootPath(util.getTestRootPath());
  string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
  string filePath = rootPath + relativePath;
  EXPECT_EQ(0, Command::getCommandStatus("hadoop fs -put -f " + filePath + " hdfs://localhost:8020/hawq_register_hawq.paq"));
  util.execute("drop table if exists nt;");
  util.execute("create table nt(i int) with (appendonly=true, orientation=parquet) distributed randomly;");
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -f hdfs://localhost:8020/hawq_register_hawq.paq nt"));
	util.query("select * from nt;", 3);
	util.execute("insert into nt values(1);");
	util.query("select * from nt;", 4);
  util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2ParquetRandomly) {
  SQLUtility util;
  util.execute("drop table if exists t;");
  util.execute("drop table if exists nt;");
  util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed randomly;");
  util.execute("insert into t values(1), (2), (3);");
  util.query("select * from t;", 3);
  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o t.yml testhawqregister_testusage2parquetrandomly.t"));
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c t.yml testhawqregister_testusage2parquetrandomly.nt"));
  util.query("select * from nt;", 3);
  EXPECT_EQ(0, Command::getCommandStatus("rm -rf t.yml"));
  util.execute("drop table t;");
  util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2ParquetHash1) {
  SQLUtility util;
  util.execute("drop table if exists t4;");
  util.execute("create table t4(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
  util.execute("insert into t4 values(1), (2), (3);");
  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o t4.yml testhawqregister_testusage2parquethash1.t4"));
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c t4.yml testhawqregister_testusage2parquethash1.nt4"));
  util.query("select * from nt4;", 3);
  EXPECT_EQ(0, Command::getCommandStatus("rm -rf t4.yml"));
  util.execute("drop table t4;");
  util.execute("drop table nt4;");
}


TEST_F(TestHawqRegister, TestUsage2ParquetHash2) {
  SQLUtility util;
  util.execute("drop table if exists t5;");
  util.execute("create table t5(i int, j varchar, k text) with (appendonly=true, orientation=parquet) distributed by (i, k);");
  util.execute("insert into t5 values(1, 'x', 'ab'), (2, 'y', 'cd'), (3, 'z', 'ef');");
  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o t5.yml testhawqregister_testusage2parquethash2.t5"));
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c t5.yml testhawqregister_testusage2parquethash2.nt5"));
  util.query("select * from nt5;", 3);
  EXPECT_EQ(0, Command::getCommandStatus("rm -rf t5.yml"));
  util.execute("drop table t5;");
  util.execute("drop table nt5;");
}


TEST_F(TestHawqRegister, TestUsage2AORandom) {
  SQLUtility util;
  util.execute("drop table if exists t6;");
  util.execute("create table t6(i int) with (appendonly=true, orientation=row) distributed randomly;");
  util.execute("insert into t6 values(1), (2), (3);");
  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o t6.yml testhawqregister_testusage2aorandom.t6"));
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c t6.yml testhawqregister_testusage2aorandom.nt6"));
  util.query("select * from nt6;", 3);
  EXPECT_EQ(0, Command::getCommandStatus("rm -rf t6.yml"));
  util.execute("drop table t6;");
  util.execute("drop table nt6;");
}

TEST_F(TestHawqRegister, TestUsage2AOHash1) {
  SQLUtility util;
  util.execute("drop table if exists t7;");
  util.execute("create table t7(i int) with (appendonly=true, orientation=row) distributed by (i);");
  util.execute("insert into t7 values(1), (2), (3);");
  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o t7.yml testhawqregister_testusage2aohash1.t7"));
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c t7.yml testhawqregister_testusage2aohash1.nt7"));
  util.query("select * from nt7;", 3);
  EXPECT_EQ(0, Command::getCommandStatus("rm -rf t7.yml"));
  util.execute("drop table t7;");
  util.execute("drop table nt7;");
}


TEST_F(TestHawqRegister, TestUsage2AOHash2) {
  SQLUtility util;
  util.execute("drop table if exists t8;");
  util.execute("create table t8(i int, j varchar, k text) with (appendonly=true, orientation=row) distributed by (i, k);");
  util.execute("insert into t8 values(1, 'x', 'ab'), (2, 'y', 'cd'), (3, 'z', 'ef');");
  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o t8.yml testhawqregister_testusage2aohash2.t8"));
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c t8.yml testhawqregister_testusage2aohash2.nt8"));
  util.query("select * from nt8;", 3);
  EXPECT_EQ(0, Command::getCommandStatus("rm -rf t8.yml"));
  util.execute("drop table t8;");
  util.execute("drop table nt8;");
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

TEST_F(TestHawqRegister, TestUsage2Behavior2) {
  SQLUtility util;
  util.execute("drop table if exists simple_register_table;");
  util.execute("create table simple_register_table(i int) with (appendonly=true, orientation=row) distributed randomly;");
  util.execute("insert into simple_register_table values(1), (2), (3);");

  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o tmp.yml testhawqregister_testusage2behavior2.simple_register_table"));
  EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c tmp.yml testhawqregister_testusage2behavior2.new_simple_register_table"));
  util.query("select * from new_simple_register_table;", 3);

  EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o new_tmp.yml testhawqregister_testusage2behavior2.new_simple_register_table"));
  EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c new_tmp.yml testhawqregister_testusage2behavior2.new_simple_register_table"));

  EXPECT_EQ(0, Command::getCommandStatus("rm -rf tmp.yml"));
  EXPECT_EQ(0, Command::getCommandStatus("rm -rf new_tmp.yml"));
  util.execute("drop table simple_register_table;");
  util.execute("drop table new_simple_register_table;");
}
