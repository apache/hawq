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

TEST_F(TestHawqRegister, TestUsage2Case2Expected) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    std::vector<string> create_table_matrix = {"distributed by (i)", "distributed randomly"};
    std::vector<string> fmt_matrix = {"row", "parquet"};
    std::vector<string> option_matrix = {"--force", "-F"};
    int suffix=0;

    for (auto & opt : option_matrix) {
        suffix = 0;
        for (auto & ddl : create_table_matrix) {
            for (auto & fmt : fmt_matrix) {
                suffix++;
                string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case2/t_tpl_%s.yml", test_root.c_str(), std::to_string(suffix).c_str()));
                string t_yml_tpl_new(hawq::test::stringFormat("%s/ManagementTool/usage2case2/t_tpl_new_%s.yml", test_root.c_str(), std::to_string(suffix).c_str()));
                string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case2/t_%s.yml", test_root.c_str(), std::to_string(suffix).c_str()));
                string t_yml_new(hawq::test::stringFormat("%s/ManagementTool/usage2case2/t_new_%s.yml", test_root.c_str(), std::to_string(suffix).c_str()));
                auto t = hawq::test::stringFormat("t_usage2_case2_%s", std::to_string(suffix).c_str());
                auto nt = hawq::test::stringFormat("nt_usage2_case2_%s", std::to_string(suffix).c_str());
                util.execute(hawq::test::stringFormat("drop table if exists %s;", t.c_str()));
                util.execute(hawq::test::stringFormat("drop table if exists %s;", nt.c_str()));

                // hawq register -d hawq_feature_test -c t_#.yml nt_usage2_case2_#
                util.execute(hawq::test::stringFormat("create table %s(i int) with (appendonly=true, orientation=%s) %s;", t.c_str(), fmt.c_str(), ddl.c_str()));
                util.execute(hawq::test::stringFormat("insert into %s select generate_series(1, 100);", t.c_str()));
                util.execute(hawq::test::stringFormat("insert into %s select generate_series(101, 200);", t.c_str()));
                util.query(hawq::test::stringFormat("select * from %s;", t.c_str()), 200);
                hawq::test::FileReplace frep;
                std::unordered_map<std::string, std::string> strs_src_dst;
                strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
                strs_src_dst["@TABLE_OID@"]= getTableOid(t, "testhawqregister_testusage2case2expected");
                string hdfs_prefix;
                hawq::test::HdfsConfig hc;
                hc.getNamenodeHost(hdfs_prefix);
                strs_src_dst["@PORT@"]= hdfs_prefix;
                frep.replace(t_yml_tpl, t_yml, strs_src_dst);
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case2expected.%s", HAWQ_DB, t_yml.c_str(), nt.c_str())));
                util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 200);

                // hawq register --force/-F -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
                util.execute(hawq::test::stringFormat("drop table if exists %s;", t.c_str()));
                util.execute(hawq::test::stringFormat("create table %s(i int) with (appendonly=true, orientation=%s) %s;", t.c_str(), fmt.c_str(), ddl.c_str()));
                util.execute(hawq::test::stringFormat("insert into %s select generate_series(1, 50);", t.c_str()));
                util.query(hawq::test::stringFormat("select * from %s;", t.c_str()), 50);
                strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
                strs_src_dst["@TABLE_OID_OLD@"]= getTableOid(nt, "testhawqregister_testusage2case2expected");
                strs_src_dst["@TABLE_OID_NEW@"]= getTableOid(t, "testhawqregister_testusage2case2expected");
                strs_src_dst["@PORT@"]= hdfs_prefix;
                frep.replace(t_yml_tpl_new, t_yml_new, strs_src_dst);
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register %s -d %s -c %s testhawqregister_testusage2case2expected.%s", opt.c_str(), HAWQ_DB, t_yml_new.c_str(), nt.c_str())));
                util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 150);

                if (fmt == "row")
                   checkPgAOSegValue(nt, "testhawqregister_testusage2case2expected", "-1", "aoseg");
                else 
                   checkPgAOSegValue(nt, "testhawqregister_testusage2case2expected", "-1", "paqseg");
                
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml_new.c_str())));
                util.execute(hawq::test::stringFormat("drop table %s;", t.c_str()));
                util.execute(hawq::test::stringFormat("drop table %s;", nt.c_str()));
            }
        }
    }
}

void TestHawqRegister::runYamlCaseForceMode(std::string casename, std::string ymlname, int isexpectederror = 1, int rows = 50, int checknum = 200, bool samepathfile = false) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), ymlname.c_str()));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), ymlname.c_str()));
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");

    util.execute("create table nt(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 200);

    // hawq register --force/-F -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
    util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute(hawq::test::stringFormat("insert into t select generate_series(1, %s);", std::to_string(rows).c_str()));
    util.query("select * from t;", rows);
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID_OLD@"]= getTableOid("nt", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    strs_src_dst["@TABLE_OID_NEW@"]= getTableOid("t", hawq::test::stringFormat("testhawqregister_%s", casename.c_str()));
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    if(samepathfile) {
        for(int i=8; i<=13; i++){
          string file(hawq::test::stringFormat("%s/ManagementTool/usage2case2/", test_root.c_str()));
          file.append(std::to_string(i));
          EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hadoop fs -put %s /hawq_default/16385/%s/%s/", file.c_str(), strs_src_dst["@DATABASE_OID@"].c_str(),strs_src_dst["@TABLE_OID_OLD@"].c_str())));
        }
    }
    //printf("%s\n", hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), casename.c_str()).c_str());
    //sleep(60);
    EXPECT_EQ(isexpectederror, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), casename.c_str())));
    util.query("select * from nt;", checknum);
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2Case2Hash2Random) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), "usage2case2/hash_to_random"));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), "usage2case2/hash_to_random"));
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");

    util.execute("create table nt(i int) with (appendonly=true, orientation=parquet) distributed randomly;");
    util.execute("insert into nt select generate_series(1, 100);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 200);

    // hawq register --force/-F -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
    util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute(hawq::test::stringFormat("insert into t select generate_series(1, %s);", std::to_string(50).c_str()));
    util.query("select * from t;", 50);
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID_OLD@"]= getTableOid("nt", "testhawqregister_testusage2case2hash2random");
    strs_src_dst["@TABLE_OID_NEW@"]= getTableOid("t", "testhawqregister_testusage2case2hash2random");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), "testusage2case2hash2random")));
    util.query("select * from nt;", 150);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2Case2Random2Hash) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), "usage2case2/random_to_hash"));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), "usage2case2/random_to_hash"));
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");

    util.execute("create table nt(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 200);

    // hawq register --force/-F -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
    util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed randomly;");
    util.execute(hawq::test::stringFormat("insert into t select generate_series(1, %s);", std::to_string(50).c_str()));
    util.query("select * from t;", 50);
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID_OLD@"]= getTableOid("nt", "testhawqregister_testusage2case2random2hash");
    strs_src_dst["@TABLE_OID_NEW@"]= getTableOid("t", "testhawqregister_testusage2case2random2hash");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), "testusage2case2random2hash")));
    util.query("select * from nt;", 200);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2Case2TableNotExist) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case2/table_not_exists_tpl.yml", test_root.c_str()));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case2/talbe_not_exists.yml", test_root.c_str()));
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");

    // hawq register --force/-F -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
    util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute(hawq::test::stringFormat("insert into t select generate_series(1, %s);", std::to_string(50).c_str()));
    util.query("select * from t;", 50);
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID_NEW@"]= getTableOid("t", "testhawqregister_testusage2case2tablenotexist");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_testusage2case2tablenotexist.nt", HAWQ_DB, t_yml.c_str())));
    util.query("select * from nt;", 50);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2Case2TableExistNoData) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case2/table_exist_no_data_tpl.yml", test_root.c_str()));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case2/table_exist_no_data.yml", test_root.c_str()));
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");

    // hawq register --force/-F -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
    util.execute("create table t(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    util.execute(hawq::test::stringFormat("insert into t select generate_series(1, %s);", std::to_string(50).c_str()));
    util.query("select * from t;", 50);
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID_NEW@"]= getTableOid("t", "testhawqregister_testusage2case2tableexistnodata");
    util.execute("create table nt(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_testusage2case2tableexistnodata.nt", HAWQ_DB, t_yml.c_str())));
    util.query("select * from nt;", 50);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2Case2NormalYamlConfig) {
    runYamlCaseForceMode("testusage2case2normalyamlconfig", "usage2case2/normal_yaml_config", 0, 50, 150);
}

TEST_F(TestHawqRegister, TestUsage2Case2SamePathYamlConfig) {
    runYamlCaseForceMode("testusage2case2samepathyamlconfig", "usage2case2/same_path_yaml_config", 0, 50, 332, true);
}

TEST_F(TestHawqRegister, TestUsage2Case2NormalYamlNoUpdateConfig) {
    runYamlCaseForceMode("testusage2case2normalyamlnoupdateconfig", "usage2case2/normal_yaml_no_update_config", 0, 50, 100);
}

TEST_F(TestHawqRegister, TestUsage2Case2FileNotIncludedInYamlConfig) {
    runYamlCaseForceMode("testusage2case2filenotincludedinyamlconfig", "usage2case2/file_not_included_in_yaml_config");
}

TEST_F(TestHawqRegister, TestUsage2Case2FileInYamlNotExist) {
    runYamlCaseForceMode("testusage2case2fileinyamlnotexist", "usage2case2/file_in_yaml_not_exist");
}

TEST_F(TestHawqRegister, TestUsage2Case2ErrorColumnNum) {
    runYamlCaseForceMode("testusage2case2errorcolumnnum", "usage2case2/error_columnnum");
}

TEST_F(TestHawqRegister, TestUsage2Case2HDFSFilePathContainErrorSymbol) {
    runYamlCaseForceMode("testusage2case2hdfsfilepathcontainerrorsymbol", "usage2case2/contain_error_symbol");
}

TEST_F(TestHawqRegister, TestUsage2Case2ZeroEof) {
    runYamlCaseForceMode("testusage2case2zeroeof", "usage2case2/zero_eof", 0, 50, 143);
}

TEST_F(TestHawqRegister, TestUsage2Case2LargerEof) {
    runYamlCaseForceMode("testusage2case2largereof", "usage2case2/larger_eof");
}

TEST_F(TestHawqRegister, TestUsage2Case2FloatEof) {
    runYamlCaseForceMode("testusage2case2floateof", "usage2case2/float_eof");
}

TEST_F(TestHawqRegister, TestUsage2Case2MinusEof) {
    runYamlCaseForceMode("testusage2case2minuseof", "usage2case2/minus_eof");
}

TEST_F(TestHawqRegister, TestUsage2Case2WrongDistributionPolicy) {
    runYamlCaseForceMode("testusage2case2wrongdistributionpolicy", "usage2case2/wrong_distributed_policy");
}

TEST_F(TestHawqRegister, TestUsage2Case2Bucket0) {
    runYamlCaseForceMode("testusage2case2bucket0", "usage2case2/bucket0");
}

TEST_F(TestHawqRegister, TestUsage2Case2ErrorEncoding) {
    runYamlCaseForceMode("testusage2case2errorencoding", "usage2case2/error_encoding");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case2ErrorBlockSize) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/%s_tpl.yml", test_root.c_str(), "usage2case2/error_blocksize"));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/%s.yml", test_root.c_str(), "usage2case2/error_blocksize"));
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");

    util.execute("create table nt(i int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 200);

    // hawq register --force/-F -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
    util.execute("create table t(i int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute(hawq::test::stringFormat("insert into t select generate_series(1, %s);", std::to_string(50).c_str()));
    util.query("select * from t;", 50);
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID_OLD@"]= getTableOid("nt", "testhawqregister_testusage2case2errorblocksize");
    strs_src_dst["@TABLE_OID_NEW@"]= getTableOid("t", "testhawqregister_testusage2case2errorblocksize");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_%s.nt", HAWQ_DB, t_yml.c_str(), "testusage2case2errorblocksize")));
    util.query("select * from nt;", 150);

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case2ErrorCompressType) {
    runYamlCaseForceMode("testusage2case2errorcompresstype", "usage2case2/error_compresstype");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case2ErrorCompressLevel) {
    runYamlCaseForceMode("testusage2case2errorcompresslevel", "usage2case2/error_compresslevel");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case2ErrorChecksum) {
    runYamlCaseForceMode("testusage2case2errorchecksum", "usage2case2/error_checksum");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case2ErrorPageSize) {
    runYamlCaseForceMode("testusage2case2errorpagesize", "usage2case2/error_pagesize");
}

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case2ErrorRowgroupSize) {
    runYamlCaseForceMode("testusage2case2errorrowgroupsize", "usage2case2/error_rowgroupsize");
}

TEST_F(TestHawqRegister, TestUsage2Case2IncludeDirectory) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hadoop fs -put -f %s/ManagementTool/usage2case2 %s/", test_root.c_str(), getHdfsLocation().c_str())));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case2/includedirectory.yml", test_root.c_str()));
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_testusage2case2includedirectory.nt", HAWQ_DB, t_yml.c_str())));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hdfs dfs -rm -r %s/usage2case2", getHdfsLocation().c_str())));
}

TEST_F(TestHawqRegister, TestUsage2Case2ErrorFormat) {
    runYamlCaseForceMode("testusage2case2errorformat", "usage2case2/error_format");
}

