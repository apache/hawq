#include <vector>
#include <string>

#include "gtest/gtest.h"
#include "lib/command.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"
#include "lib/hdfs_config.h"
#include "test_hawq_register.h"

using std::vector;
using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;
using hawq::test::HdfsConfig;

TEST_F(TestHawqRegister, TestUsage1ExpectSuccess) {
  // Register file/folder into HAWQ by specific file/folder name

  SQLUtility util;
  string rootPath(util.getTestRootPath());
  string filePath = rootPath + "/ManagementTool/data/parquet200/dat.paq";
  string folderPath = rootPath + "/ManagementTool/data/parquet200sum/";

  vector<string> ddl_orientation_matrix = {"parquet"};
  vector<string> distribution_policy_matrix = {"", "DISTRIBUTED RANDOMLY"};
  vector<string> folder_matrix = {"/usage1tmp/", "/usage1tmp"};
  
  for(int i = 0; i < ddl_orientation_matrix.size() * distribution_policy_matrix.size() * 2 + ddl_orientation_matrix.size() * distribution_policy_matrix.size() * folder_matrix.size(); ++i) {
    util.execute(hawq::test::stringFormat("drop table if exists t_%s;", std::to_string(i).c_str()));
  }
  auto register_lambda = [&] () {
    int suffix = 0;
    // hawq register -d hawq_feature_test -f hdfs://localhost:8020/usage1dat.paq t_#
    for(auto & ddl : ddl_orientation_matrix) {
      for(auto & policy : distribution_policy_matrix) {
        auto cmd = hawq::test::stringFormat("hdfs dfs -put -f %s %s/usage1dat.paq", filePath.c_str(), getHdfsLocation().c_str());
        EXPECT_EQ(0, Command::getCommandStatus(cmd));
        
        auto sql = hawq::test::stringFormat("CREATE TABLE t_%s(i int) with (appendonly=true, orientation=%s) %s;", std::to_string(suffix).c_str(), ddl.c_str(), policy.c_str());
        util.execute(sql); util.query(hawq::test::stringFormat("SELECT * from t_%s", std::to_string(suffix).c_str()), 0);
        
        cmd = hawq::test::stringFormat("hawq register -d %s -f %s/usage1dat.paq t_%s", HAWQ_DB, getHdfsLocation().c_str(), std::to_string(suffix).c_str());
        EXPECT_EQ(0, Command::getCommandStatus(cmd));
        
        util.query(hawq::test::stringFormat("select * from t_%s;", std::to_string(suffix).c_str()), 200);
        util.execute(hawq::test::stringFormat("insert into t_%s values(201);", std::to_string(suffix).c_str()));
        util.query(hawq::test::stringFormat("select * from t_%s;", std::to_string(suffix).c_str()), 201);
        
        suffix ++;
      }
    }

    // hawq register -d hawq_feature_test -f hdfs://localhost:8020/usage1dat.paq -e eof t_#
    for(auto & ddl : ddl_orientation_matrix) {
      for(auto & policy : distribution_policy_matrix) {
        auto cmd = hawq::test::stringFormat("hdfs dfs -put -f %s %s/usage1dat.paq", filePath.c_str(), getHdfsLocation().c_str());
        EXPECT_EQ(0, Command::getCommandStatus(cmd));
        
        auto sql = hawq::test::stringFormat("CREATE TABLE t_%s(i int) with (appendonly=true, orientation=%s) %s;", std::to_string(suffix).c_str(), ddl.c_str(), policy.c_str());
        util.execute(sql); util.query(hawq::test::stringFormat("SELECT * from t_%s", std::to_string(suffix).c_str()), 0);
        
        cmd = hawq::test::stringFormat("hawq register -d %s -f %s/usage1dat.paq -e 596 t_%s", HAWQ_DB, getHdfsLocation().c_str(), std::to_string(suffix).c_str());
        EXPECT_EQ(0, Command::getCommandStatus(cmd));
        
        util.query(hawq::test::stringFormat("select * from t_%s;", std::to_string(suffix).c_str()), 100);
        util.execute(hawq::test::stringFormat("insert into t_%s values(101);", std::to_string(suffix).c_str()));
        util.query(hawq::test::stringFormat("select * from t_%s;", std::to_string(suffix).c_str()), 101);
        
        suffix ++;
      }
    }

    // hawq register -d hawq_feature_test -f hdfs://localhost:8020/usage1tmp/ t_#
    // hawq register -d hawq_feature_test -f hdfs://localhost:8020/usage1tmp t_#
    for(auto & ddl : ddl_orientation_matrix) {
      for(auto & policy : distribution_policy_matrix) {
        for(auto & folder : folder_matrix) {
          auto cmd = hawq::test::stringFormat("hdfs dfs -mkdir -p %s/usage1tmp/", getHdfsLocation().c_str());
          EXPECT_EQ(0, Command::getCommandStatus(cmd));
          cmd = hawq::test::stringFormat("hdfs dfs -put -f %s/*.paq %s/usage1tmp/", folderPath.c_str(), getHdfsLocation().c_str());
          EXPECT_EQ(0, Command::getCommandStatus(cmd));
        
          auto sql = hawq::test::stringFormat("CREATE TABLE t_%s(i int) with (appendonly=true, orientation=%s) %s;", std::to_string(suffix).c_str(), ddl.c_str(), policy.c_str());
          util.execute(sql); util.query(hawq::test::stringFormat("SELECT * from t_%s", std::to_string(suffix).c_str()), 0);
        
          cmd = hawq::test::stringFormat("hawq register -d %s -f %s%s t_%s", HAWQ_DB, getHdfsLocation().c_str(), folder.c_str(), std::to_string(suffix).c_str());
          EXPECT_EQ(0, Command::getCommandStatus(cmd));
    
          util.query(hawq::test::stringFormat("select * from t_%s;", std::to_string(suffix).c_str()), 200);
          util.execute(hawq::test::stringFormat("insert into t_%s values(201);", std::to_string(suffix).c_str()));
          util.query(hawq::test::stringFormat("select * from t_%s;", std::to_string(suffix).c_str()), 201);
        
          suffix ++;
        }
      }
    }
    
  }; // register_lambda

  auto gc_lambda = [&] () {
    auto sql = hawq::test::stringFormat("hdfs dfs -rm -r %s/usage1tmp/", getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(sql));
    for(int i = 0; i < ddl_orientation_matrix.size() * distribution_policy_matrix.size() * 2 + ddl_orientation_matrix.size() * distribution_policy_matrix.size() * folder_matrix.size(); ++i) {
      util.execute(hawq::test::stringFormat("drop table t_%s;", std::to_string(i).c_str()));
    }
  };

  register_lambda();
  gc_lambda();
}
