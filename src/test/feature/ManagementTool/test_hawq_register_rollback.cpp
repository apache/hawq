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

TEST_F(TestHawqRegister, TestRollbackErrorSchema) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=row) distributed randomly;");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    util.execute("create table nt(i int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/rollback/error_schema.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/rollback/error_schema.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", "testhawqregister_testrollbackerrorschema");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testrollbackerrorschema.nt", HAWQ_DB, t_yml.c_str())));
    util.query("select * from t;", 100);
    util.query("select * from nt;", 100);
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestRollbackErrorHDFSFileSize) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=row) distributed randomly;");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    util.execute("create table nt(i int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/rollback/error_schema.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/rollback/error_schema.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", "testhawqregister_testrollbackerrorhdfsfilesize");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testrollbackerrorhdfsfilesize.nt", HAWQ_DB, t_yml.c_str())));
    util.query("select * from t;", 100);
    util.query("select * from nt;", 100);
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}

TEST_F(TestHawqRegister, TestRollBackHDFSFilePathContainErrorSymbol) {
    SQLUtility util;
    string test_root(util.getTestRootPath());
    util.execute("drop table if exists t;");
    util.execute("drop table if exists nt;");
    
    util.execute("create table t(i int) with (appendonly=true, orientation=row) distributed randomly;");
    util.execute("insert into t select generate_series(1, 100);");
    util.query("select * from t;", 100);
    util.execute("create table nt(i int) with (appendonly=true, orientation=row) distributed by (i);");
    util.execute("insert into nt select generate_series(1, 100);");
    util.query("select * from nt;", 100);
    string t_yml(hawq::test::stringFormat("%s/ManagementTool/rollback/error_schema.yml", test_root.c_str()));
    string t_yml_tpl(hawq::test::stringFormat("%s/ManagementTool/rollback/error_schema.yml", test_root.c_str()));
    hawq::test::FileReplace frep;
    std::unordered_map<std::string, std::string> strs_src_dst;
    strs_src_dst["@DATABASE_OID@"]= getDatabaseOid();
    strs_src_dst["@TABLE_OID@"]= getTableOid("t", "testhawqregister_testrollbackhdfsfilepathcontainerrorsymbol");
    string hdfs_prefix;
    hawq::test::HdfsConfig hc;
    hc.getNamenodeHost(hdfs_prefix);
    strs_src_dst["@PORT@"]= hdfs_prefix;
    frep.replace(t_yml_tpl, t_yml, strs_src_dst);
    EXPECT_EQ(1, Command::getCommandStatus(hawq::test::stringFormat("hawq register -d %s -c %s testhawqregister_testrollbackhdfsfilepathcontainerrorsymbol.nt", HAWQ_DB, t_yml.c_str())));
    util.query("select * from t;", 100);
    util.query("select * from nt;", 100);
    
    EXPECT_EQ(0, Command::getCommandStatus(hawq::test::stringFormat("rm -rf %s", t_yml.c_str())));
    util.execute("drop table t;");
    util.execute("drop table nt;");
}
