#include <string>

#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"
#include "lib/hdfs_config.h"
#include "lib/file_replace.h"
#include "test_hawq_register.h"

#include "gtest/gtest.h"

using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;
using hawq::test::HdfsConfig;

TEST_F(TestHawqRegister, TestUsage2Case1EmptyTable) {
    SQLUtility util;
    util.execute("drop table if exists t9;");
    util.execute("create table t9(i int) with (appendonly=true, orientation=row) distributed randomly;");
    EXPECT_EQ(0, Command::getCommandStatus("hawq extract -d " + (string) HAWQ_DB + " -o t9.yml testhawqregister_testusage2case1emptytable.t9"));
    EXPECT_EQ(0, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c t9.yml testhawqregister_testusage2case1emptytable.nt9"));
    util.query("select * from nt9;", 0);
    std::string reloid = getTableOid("nt9", "testhawqregister_testusage2case1emptytable");
    /* An empty table has no row in pg_aoseg.pg_aoseg_xxx table */
    util.query(hawq::test::stringFormat("select * from pg_aoseg.pg_aoseg_%s;", reloid.c_str()), 0);
    EXPECT_EQ(0, Command::getCommandStatus("rm -rf t9.yml"));
    util.execute("drop table t9;");
    util.execute("drop table nt9;");
}

TEST_F(TestHawqRegister, TestUsage2Case1IncorrectYaml) {
    SQLUtility util;
    string filePath = util.getTestRootPath() + "/ManagementTool/";

    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_pagesize.yml testhawqregister_testusage2case1incorrectyaml.xx"));
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_rowgroupsize.yml testhawqregister_testusage2case1incorrectyaml.xx"));
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_filesize.yml testhawqregister_testusage2case1incorrectyaml.xx"));
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "wrong_schema.yml testhawqregister_testusage2case1incorrectyaml.xx"));
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_checksum.yml testhawqregister_testusage2case1incorrectyaml.xx"));
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "wrong_dfs_url.yml testhawqregister_testusage2case1incorrectyaml.xx"));
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_bucketnum.yml testhawqregister_testusage2case1incorrectyaml.xx"));
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "missing_constraint_partition.yml testhawqregister_testusage2case1incorrectyaml.xx"));
}

TEST_F(TestHawqRegister, TestUsage2Case1MismatchFileNumber) {
    SQLUtility util;
    string filePath = util.getTestRootPath() + "/ManagementTool/";
    EXPECT_EQ(1, Command::getCommandStatus("hawq register -d " + (string) HAWQ_DB + " -c " + filePath + "files_incomplete.yml testhawqregister_testusage2case1mismatchfilenumber.xx"));
}


TEST_F(TestHawqRegister, TestUsage2Case1Expected) {
    SQLUtility util;
    string fmt_prefix;
    std::vector<string> create_table_matrix = {"distributed by (i)", "distributed randomly"};
    std::vector<string> fmt_matrix = {"row", "parquet"};
    int suffix=0;

    for (auto & ddl : create_table_matrix) {
        for (auto & fmt : fmt_matrix) {
            if (fmt.compare("row") == 0)
                fmt_prefix = "aoseg";
            else
                fmt_prefix = "paqseg";

            suffix++;
            auto t = hawq::test::stringFormat("t_usage2_case1_%s", std::to_string(suffix).c_str());
            auto nt = hawq::test::stringFormat("nt_usage2_case1_%s", std::to_string(suffix).c_str());
            util.execute(hawq::test::stringFormat("drop table if exists %s;", t.c_str()));
            util.execute(hawq::test::stringFormat("drop table if exists %s;", nt.c_str()));

            // hawq register -d hawq_feature_test -c t_usage2_case1_#.yml nt_usage2_case1_#, where nt_usage2_case1_# does not exist
            util.execute(hawq::test::stringFormat("create table %s(i int) with (appendonly=true, orientation=%s) %s;", t.c_str(), fmt.c_str(), ddl.c_str()));
            util.execute(hawq::test::stringFormat("insert into %s select generate_series(1, 100);", t.c_str()));
            util.query(hawq::test::stringFormat("select * from %s;", t.c_str()), 100);

            // get pg_aoseg.pg_xxxseg_xxx table
            std::string reloid1 = getTableOid(t.c_str(), "testhawqregister_testusage2case1expected");
            string result1 = util.getQueryResultSetString(hawq::test::stringFormat("select * from pg_aoseg.pg_%s_%s order by segno;", fmt_prefix.c_str(), reloid1.c_str()));

            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_%s.yml testhawqregister_testusage2case1expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), t.c_str())));
            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_%s.yml testhawqregister_testusage2case1expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), nt.c_str())));
            util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 100);

            // check pg_aoseg.pg_xxxseg_xxx table
            std::string reloid2 = getTableOid(nt.c_str(), "testhawqregister_testusage2case1expected");
            string result2 = util.getQueryResultSetString(hawq::test::stringFormat("select * from pg_aoseg.pg_%s_%s order by segno;", fmt_prefix.c_str(), reloid2.c_str()));
            EXPECT_EQ(result1, result2);

            // hawq register -d hawq_feature_test -c t_usage2_case1_#.yml nt_usage2_case1_#, where nt_usage2_case1_# exists
            util.execute(hawq::test::stringFormat("drop table if exists %s;", t.c_str()));
            util.execute(hawq::test::stringFormat("create table %s(i int) with (appendonly=true, orientation=%s) %s;", t.c_str(), fmt.c_str(), ddl.c_str()));
            util.execute(hawq::test::stringFormat("insert into %s select generate_series(101, 150);", t.c_str()));
            util.query(hawq::test::stringFormat("select * from %s;", t.c_str()), 50);
            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_%s.yml testhawqregister_testusage2case1expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), t.c_str())));
            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_%s.yml testhawqregister_testusage2case1expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), nt.c_str())));
            util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 150);

            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf t_%s.yml", std::to_string(suffix).c_str())));
            util.execute(hawq::test::stringFormat("drop table %s;", t.c_str()));
            util.execute(hawq::test::stringFormat("drop table %s;", nt.c_str()));
        }
    }
}

void TestHawqRegister::runYamlCaseTableExists(std::string casename, std::string ymlname, int isexpectederror=1, int checknum=100) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    util.execute("create table nt(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), ymlname.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), ymlname.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    hawq::test::HdfsConfig hc;
    string hdfs_prefix;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    //printf("%s\n", hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), casename.c_str()).c_str());
    //sleep(60);
    EXPECT_EQ(isexpectederror, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), casename.c_str())));
    if (isexpectederror > 0) {
        util.query("select * from t;", 100);
        util.query("select * from nt;", 100);
    } else {
        util.query("select * from nt;", checknum);
    }
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

void TestHawqRegister::runYamlCaseTableNotExists(std::string casename, std::string ymlname, int isexpectederror=1, int checknum=100) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), ymlname.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), ymlname.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    hawq::test::HdfsConfig hc;
    string hdfs_prefix;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(isexpectederror, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), casename.c_str())));
    if (isexpectederror > 0) {
        util.query("select * from t;", 100);
        std::string relnamespace = "2200";
        const hawq::test::PSQLQueryResult &result_tmp = conn->getQueryResult(
            hawq::test::stringFormat("SELECT oid from pg_namespace where nspname= \'testhawqregister_%s\';", casename.c_str()));
        std::vector<std::vector<std::string>> table = result_tmp.getRows();
        if (table.size() > 0) {
            relnamespace = table[0][0];
        }
        util.query(hawq::test::stringFormat("select * from pg_class where relnamespace = %s and relname = 'nt';", relnamespace.c_str()), 0);
    } else {
        util.query("select * from nt;", checknum);
    }
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table if exists nt;");
}

TEST_F(TestHawqRegister, TestUsage2Case1ErrorSchema) {
    runYamlCaseTableNotExists("testusage2case1errorschema", "usage2case1/error_schema");
}

TEST_F(TestHawqRegister, TestUsage2Case1ErrorColumnNum) {
    runYamlCaseTableExists("testusage2case1errorcolumnnum", "usage2case1/error_columnnum");
}

TEST_F(TestHawqRegister, TestUsage2Case1HDFSFilePathContainErrorSymbol) {
    runYamlCaseTableExists("testusage2case1hdfsfilepathcontainerrorsymbol", "usage2case1/contain_error_symbol");
}

TEST_F(TestHawqRegister, TestUsage2Case1ErrorHashTableRegistry) {
    SQLUtility util;
    util.execute("drop table if exists t_1_1;");
    util.execute("drop table if exists t_1_2;");
    util.execute("drop table if exists t_1_3;");
    util.execute("drop table if exists t_2;");
    util.execute("drop table if exists nt_1;");
    util.execute("drop table if exists nt_2;");

    util.execute("create table t_1_1(i int, j int, k int) with (appendonly=true, orientation=row, bucketnum=12) distributed by (i, j);");
    util.execute("insert into t_1_1 select generate_series(1, 100);");
    util.query("select * from t_1_1;", 100);
    util.execute("create table t_1_2(i int, j int, k int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute("insert into t_1_2 select generate_series(1, 100);");
    util.query("select * from t_1_2;", 100);
    util.execute("create table t_1_3(i int, j int, k int) with (appendonly=true, orientation=row) distributed randomly;");
    util.execute("insert into t_1_3 select generate_series(1, 100);");
    util.query("select * from t_1_3;", 100);
    util.execute("create table t_2(i int, j int, k int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute("insert into t_2 select generate_series(1, 100);");
    util.query("select * from t_2;", 100);
    util.execute("create table nt_1(i int, j int, k int) with (appendonly=true, orientation=row) distributed by (i, j);");
    util.execute("insert into nt_1 select generate_series(1, 100);");
    util.query("select * from nt_1;", 100);
    util.execute("create table nt_2(i int, j int, k int) with (appendonly=true, orientation=row) distributed by (j);");
    util.execute("insert into nt_2 select generate_series(1, 100);");
    util.query("select * from nt_2;", 100);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_1_1.yml testhawqregister_testusage2case1errorhashtableregistry.t_1_1", HAWQ_DB)));
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_1_1.yml testhawqregister_testusage2case1errorhashtableregistry.nt_1", HAWQ_DB)));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_1_2.yml testhawqregister_testusage2case1errorhashtableregistry.t_1_2", HAWQ_DB)));
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_1_2.yml testhawqregister_testusage2case1errorhashtableregistry.nt_1", HAWQ_DB)));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_1_3.yml testhawqregister_testusage2case1errorhashtableregistry.t_1_3", HAWQ_DB)));
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_1_3.yml testhawqregister_testusage2case1errorhashtableregistry.nt_1", HAWQ_DB)));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_2.yml testhawqregister_testusage2case1errorhashtableregistry.t_2", HAWQ_DB)));
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_2.yml testhawqregister_testusage2case1errorhashtableregistry.nt_2", HAWQ_DB)));

    EXPECT_EQ(0, Command::getCommandStatus("rm -rf t_1_1.yml"));
    EXPECT_EQ(0, Command::getCommandStatus("rm -rf t_1_2.yml"));
    EXPECT_EQ(0, Command::getCommandStatus("rm -rf t_1_3.yml"));
    EXPECT_EQ(0, Command::getCommandStatus("rm -rf t_2.yml"));
    util.execute("drop table t_1_1;");
    util.execute("drop table t_1_2;");
    util.execute("drop table t_1_3;");
    util.execute("drop table t_2;");
    util.execute("drop table nt_1;");
    util.execute("drop table nt_2;");
}

TEST_F(TestHawqRegister, TestUsage2Case1ZeroEof) {
    runYamlCaseTableExists("testusage2case1zeroeof", "usage2case1/zero_eof", 0, 180);
}

TEST_F(TestHawqRegister, TestUsage2Case1LargerEof) {
    runYamlCaseTableExists("testusage2case1largereof", "usage2case1/larger_eof");
}

TEST_F(TestHawqRegister, TestUsage2Case1FloatEof) {
    runYamlCaseTableExists("testusage2case1floateof", "usage2case1/float_eof");
}

TEST_F(TestHawqRegister, TestUsage2Case1MinusEof) {
    runYamlCaseTableExists("testusage2case1minuseof", "usage2case1/minus_eof");
}

TEST_F(TestHawqRegister, TestUsage2Case1WrongDistributionPolicy) {
    runYamlCaseTableExists("testusage2case1wrongdistributionpolicy", "usage2case1/wrong_distributed_policy");
}

TEST_F(TestHawqRegister, TestUsage2Case1Bucket0) {
    runYamlCaseTableExists("testusage2case1bucket0", "usage2case1/bucket0");
}

TEST_F(TestHawqRegister, TestUsage2Case1ErrorEncoding) {
    runYamlCaseTableExists("testusage2case1errorencoding", "usage2case1/error_encoding");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case1ErrorBlockSize) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    util.execute("create table nt(i int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case1/error_blockszie.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case1/error_blocksize_tpl.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", "testhawqregister_testusage2case1errorblocksize");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case1errorblocksize.nt", HAWQ_DB, t_yml.c_str())));
    util.query("select * from t;", 100);
    util.query("select * from nt;", 100);
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case1ErrorCompressType) {
    runYamlCaseTableExists("testusage2case1errorcompresstype", "usage2case1/error_compresstype");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case1ErrorCompressLevel) {
    runYamlCaseTableExists("testusage2case1errorcompresslevel", "usage2case1/error_compresslevel");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case1ErrorChecksum) {
    runYamlCaseTableExists("testusage2case1errorchecksum", "usage2case1/error_checksum");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case1ErrorPageSize) {
    runYamlCaseTableExists("testusage2case1errorpagesize", "usage2case1/error_pagesize");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case1ErrorRowgroupSize) {
    runYamlCaseTableExists("testusage2case1errorrowgroupsize", "usage2case1/error_rowgroupsize");
}

TEST_F(TestHawqRegister, TestUsage2Case1IncludeDirectory) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hadoop fs -put -f %s/ManagementTool/usage2case1 %s/", test_root.c_str(), getHdfsLocation().c_str())));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case1/includedirectory.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case1/includedirectory_tpl.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    hawq::test::HdfsConfig hc;
    string hdfs_prefix;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case1includedirectory.nt", HAWQ_DB, t_yml.c_str())));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hdfs dfs -rm -r %s/usage2case1", getHdfsLocation().c_str())));
}

TEST_F(TestHawqRegister, TestUsage2Case1ErrorFormat) {
    runYamlCaseTableExists("testusage2case1errorformat", "usage2case1/error_format");
}

TEST_F(TestHawqRegister, TestUsage2Case1FileUnderTableDirectory) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case1/file_under_table_directory.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case1/file_under_table_directory_tpl.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", "testhawqregister_testusage2case1fileUndertabledirectory");
    hawq::test::HdfsConfig hc;
    string hdfs_prefix;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case1fileUndertabledirectory.t", HAWQ_DB, t_yml.c_str())));
    util.query("select * from t;", 100);
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
}
