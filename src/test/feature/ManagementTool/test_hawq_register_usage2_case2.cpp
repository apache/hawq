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
    int suffix=0;

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
            strs_src_dst["@TABLE_OID@"]= getTableOid(t);
            frep.replace(t_yml_tpl, t_yml, strs_src_dst);
            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testusage2case2expected.%s", HAWQ_DB, t_yml.c_str(), nt.c_str())));
            util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 200);

            // hawq register --force -d hawq_feature_test -c t_new_#.yml nt_usage2_case2_#
            util.execute(hawq::test::stringFormat("drop table if exists %s;", t.c_str()));
            util.execute(hawq::test::stringFormat("create table %s(i int) with (appendonly=true, orientation=%s) %s;", t.c_str(), fmt.c_str(), ddl.c_str()));
            util.execute(hawq::test::stringFormat("insert into %s select generate_series(1, 50);", t.c_str()));
            util.query(hawq::test::stringFormat("select * from %s;", t.c_str()), 50);
            strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
            strs_src_dst["@TABLE_OID_OLD@"]= getTableOid(nt);
            strs_src_dst["@TABLE_OID_NEW@"]= getTableOid(t);
            frep.replace(t_yml_tpl_new, t_yml_new, strs_src_dst);
            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register --force -d %s -c %s testhawqregister_testusage2case2expected.%s", HAWQ_DB, t_yml_new.c_str(), nt.c_str())));
            util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 150);

            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
            EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml_new.c_str())));
            util.execute(hawq::test::stringFormat("drop table %s;", t.c_str()));
            util.execute(hawq::test::stringFormat("drop table %s;", nt.c_str()));
        }
    }
}

