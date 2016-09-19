#include <string>

#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"
#include "lib/hdfs_config.h"
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
            EXPECT_EQ(0, Command::getCommandStatus("rm -rf t.yml"));
            util.execute(hawq::test::stringFormat("drop table %s;", t.c_str()));
            util.execute(hawq::test::stringFormat("drop table %s;", nt.c_str()));
        }
    }
}

