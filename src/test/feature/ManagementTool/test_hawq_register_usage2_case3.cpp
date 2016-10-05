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

TEST_F(TestHawqRegister, DISABLED_TestUsage2Case3Expected) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    std::vector<string> create_table_matrix = {"distributed by (i)", "distributed randomly"};
    std::vector<string> fmt_matrix = {"row", "parquet"};
    std::vector<string> option_matrix = {"--repair", "-R"};
    int suffix=0;

    for (auto & opt : option_matrix) {
        suffix = 0;
        for (auto & ddl : create_table_matrix) {
            for (auto & fmt : fmt_matrix) {
                suffix++;
                string t_yml_tpl_old(hawq::test::stringFormat("%s/ManagementTool/usage2case3/t_tpl_old_%s.yml", test_root.c_str(), std::to_string(suffix).c_str()));
                string t_yml_old(hawq::test::stringFormat("%s/ManagementTool/usage2case3/t_old_%s.yml", test_root.c_str(), std::to_string(suffix).c_str()));
                auto t = hawq::test::stringFormat("t_usage2_case3_%s", std::to_string(suffix).c_str());
                auto nt = hawq::test::stringFormat("nt_usage2_case3_%s", std::to_string(suffix).c_str());
                util.execute(hawq::test::stringFormat("drop table if exists %s;", t.c_str()));
                util.execute(hawq::test::stringFormat("drop table if exists %s;", nt.c_str()));

                // hawq register -d hawq_feature_test -c t_usage2_case3_#.yml nt_usage2_case3_#, where nt_usage2_case3_# does not exist
                util.execute(hawq::test::stringFormat("create table %s(i int) with (appendonly=true, orientation=%s) %s;", t.c_str(), fmt.c_str(), ddl.c_str()));
                util.execute(hawq::test::stringFormat("insert into %s select generate_series(1, 100);", t.c_str()));
                util.execute(hawq::test::stringFormat("insert into %s select generate_series(101, 200);", t.c_str()));
                util.query(hawq::test::stringFormat("select * from %s;", t.c_str()), 200);
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_%s.yml testhawqregister_testusage2case3expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), t.c_str())));
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_%s.yml testhawqregister_testusage2case3expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), nt.c_str())));
                util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 200);

                // hawq register -d hawq_feature_test -c t_usage2_case3_#.yml nt_usage2_case3_#, where nt_usage2_case3_# exists
                util.execute(hawq::test::stringFormat("drop table if exists %s;", t.c_str()));
                util.execute(hawq::test::stringFormat("create table %s(i int) with (appendonly=true, orientation=%s) %s;", t.c_str(), fmt.c_str(), ddl.c_str()));
                util.execute(hawq::test::stringFormat("insert into %s select generate_series(101, 150);", t.c_str()));
                util.query(hawq::test::stringFormat("select * from %s;", t.c_str()), 50);
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq extract -d %s -o t_%s.yml testhawqregister_testusage2case3expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), t.c_str())));
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c t_%s.yml testhawqregister_testusage2case3expected.%s", HAWQ_DB, std::to_string(suffix).c_str(), nt.c_str())));
                util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 250);

                // hawq register --repair/-R -d hawq_feature_test -c t_old_#.yml nt_usage2_case3_#
                hawq::test::FileReplace frep;
                std::unordered_map<std::string, std::string> strs_src_dst;
                strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
                strs_src_dst["@TABLE_OID@"]= getTableOid(nt);
                frep.replace(t_yml_tpl_old, t_yml_old, strs_src_dst);
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("hawq register %s -d %s -c %s testhawqregister_testusage2case3expected.%s", opt.c_str(), HAWQ_DB, t_yml_old.c_str(), nt.c_str())));
                util.query(hawq::test::stringFormat("select * from %s;", nt.c_str()), 100);

                if (fmt == "row")
                   checkPgAOSegValue(nt, "-1", "aoseg");
                else 
                   checkPgAOSegValue(nt, "-1", "paqseg");
                
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf t_%s.yml", std::to_string(suffix).c_str())));
                EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml_old.c_str())));
                util.execute(hawq::test::stringFormat("drop table %s;", t.c_str()));
                util.execute(hawq::test::stringFormat("drop table %s;", nt.c_str()));
            }
        }
    }
}

