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

TEST_F(TestHawqRegister, TestUsage2Case1Expected) {
    SQLUtility util;
    std::vector<string> create_table_matrix = {"distributed by (i)", "distributed randomly"};
    std::vector<string> fmt_matrix = {"row", "parquet"};
    int suffix=0;
    for (auto & ddl : create_table_matrix) {
        for (auto & fmt : fmt_matrix) {
            suffix++;
            auto t = hawq::test::stringFormat("t_usage2_case1_%s", std::to_string(suffix).c_str());
            auto nt = hawq::test::stringFormat("nt_usage2_case1_%s", std::to_string(suffix).c_str());
            util.execute(hawq::test::stringFormat("drop table if exists %s;", t.c_str()));
            util.execute(hawq::test::stringFormat("drop table if exists %s;", nt.c_str()));

            // hawq register -d hawq_feature_test -c t_usage2_case1_#.yml nt_usage2_case1_#, where nt_usage2_case1_# does not exist
            util.execute(hawq::test::stringFormat("create table %s(i int) with (appendonly=true, orientation=%s) %s;", t.c_str(), fmt.c_str(), ddl.c_str()));
            util.execute(hawq::test::stringFormat("insert into %s select generate_series(1, 100);", t.c_str()));
            util.query(hawq::test::stringFormat("select * from %s;", t.c_str()), 100);
            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_%s.yml testhawqregister_testusage2case1expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), t.c_str())));
            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_%s.yml testhawqregister_testusage2case1expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), nt.c_str())));
            util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 100);

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

TEST_F(TestHawqRegister, TestUsage2Case1ErrorEncoding) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=row) distributed randomly;");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    util.execute("create table nt(i int) with (appendonly=true, orientation=row) distributed randomly;");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case1/error_encoding.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case1/error_encoding_tpl.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t");
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case1errorencoding.nt", HAWQ_DB, t_yml.c_str())));
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2Case1Bucket0) {
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
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case1/bucket0.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case1/bucket0_tpl.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t");
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case1bucket0.nt", HAWQ_DB, t_yml.c_str())));
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestUsage2Case1IncludeDirectory) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hdfs dfs -put -f %s/ManagementTool/usage2case1 %s/", test_root.c_str(), getHdfsLocation().c_str())));
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case1/includedirectory.yml", test_root.c_str()));
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case1includedirectory.nt", HAWQ_DB, t_yml.c_str())));
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hdfs dfs -rm -r %s/usage2case1", getHdfsLocation().c_str())));
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

TEST_F(TestHawqRegister, TestUsage2Case1LargerEof) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=row) distributed randomly;");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case1/larger_eof.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case1/larger_eof_tpl.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t");
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case1largereof.nt", HAWQ_DB, t_yml.c_str())));

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
}


TEST_F(TestHawqRegister, TestUsage2Case1WrongDistributionPolicy) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=row) distributed randomly;");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/usage2case1/wrong_distributed_policy.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/usage2case1/wrong_distributed_policy_tpl.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t");
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case1wrongdistributionpolicy.nt", HAWQ_DB, t_yml.c_str())));

    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
}
