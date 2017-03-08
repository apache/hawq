/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

TEST_F(TestHawqRegister, TestUsage1ExpectSuccessDifferentSchema) {
  SQLUtility util;
  string rootPath(util.getTestRootPath());
  string filePath = rootPath + "/ManagementTool/data/parquet200/dat.paq";

  util.execute("drop schema if exists xz;");
  util.execute("create schema xz;");
  util.execute("drop table if exists t;");
  auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1esdsusage1dat.paq", filePath.c_str(), getHdfsLocation().c_str());
  EXPECT_EQ(0, Command::getCommandStatus(cmd));
  std::string policy = "DISTRIBUTED RANDOMLY";;
  auto sql1 = hawq::test::stringFormat("CREATE TABLE t(i int) with (appendonly=true, orientation=parquet) %s;", policy.c_str());
  auto sql2 = hawq::test::stringFormat("CREATE TABLE xz.t(i int) with (appendonly=true, orientation=parquet) %s;", policy.c_str());
  util.execute(sql1); util.execute(sql2);
  util.query("SELECT * from t", 0);
  cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1esdsusage1dat.paq testhawqregister_testusage1expectsuccessdifferentschema.t", HAWQ_DB, getHdfsLocation().c_str());
  EXPECT_EQ(0, Command::getCommandStatus(cmd));
  util.query("select * from t;", 200);
  util.execute("insert into t values(201);");
  util.query("select * from t;", 201);
  util.execute("drop table xz.t;");
  util.execute("drop table t;");
  util.execute("drop schema xz;");
}

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
        auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1esusage1dat.paq", filePath.c_str(), getHdfsLocation().c_str());
        EXPECT_EQ(0, Command::getCommandStatus(cmd));
        
        auto sql = hawq::test::stringFormat("CREATE TABLE t_%s(i int) with (appendonly=true, orientation=%s) %s;", std::to_string(suffix).c_str(), ddl.c_str(), policy.c_str());
        util.execute(sql); util.query(hawq::test::stringFormat("SELECT * from t_%s", std::to_string(suffix).c_str()), 0);
        
        cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1esusage1dat.paq testhawqregister_testusage1expectsuccess.t_%s", HAWQ_DB, getHdfsLocation().c_str(), std::to_string(suffix).c_str());
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
        auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1esusage1dat.paq", filePath.c_str(), getHdfsLocation().c_str());
        EXPECT_EQ(0, Command::getCommandStatus(cmd));
        
        auto sql = hawq::test::stringFormat("CREATE TABLE t_%s(i int) with (appendonly=true, orientation=%s) %s;", std::to_string(suffix).c_str(), ddl.c_str(), policy.c_str());
        util.execute(sql); util.query(hawq::test::stringFormat("SELECT * from t_%s", std::to_string(suffix).c_str()), 0);
        
        cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1esusage1dat.paq -e 596 testhawqregister_testusage1expectsuccess.t_%s", HAWQ_DB, getHdfsLocation().c_str(), std::to_string(suffix).c_str());
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
          cmd = hawq::test::stringFormat("hadoop fs -put -f %s/*.paq %s/usage1tmp/", folderPath.c_str(), getHdfsLocation().c_str());
          EXPECT_EQ(0, Command::getCommandStatus(cmd));
        
          auto sql = hawq::test::stringFormat("CREATE TABLE t_%s(i int) with (appendonly=true, orientation=%s) %s;", std::to_string(suffix).c_str(), ddl.c_str(), policy.c_str());
          util.execute(sql); util.query(hawq::test::stringFormat("SELECT * from t_%s", std::to_string(suffix).c_str()), 0);
        
          cmd = hawq::test::stringFormat("hawq register -d %s -f %s%s testhawqregister_testusage1expectsuccess.t_%s", HAWQ_DB, getHdfsLocation().c_str(), folder.c_str(), std::to_string(suffix).c_str());
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

TEST_F(TestHawqRegister, TestUsage1SingleHawqFile) {
    SQLUtility util;
    string rootPath(util.getTestRootPath());
    string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
    string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1shfhawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
    util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1shfhawq_register_hawq.paq testhawqregister_testusage1singlehawqfile.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
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

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1shfhawq_register_hive.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
    util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1shfhawq_register_hive.paq testhawqregister_testusage1singlehivefile.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
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

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tdthawq_register_data_types.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    util.execute("create table hawqregister(a bool, b int2, c int2, d int4, e int8, f date, g float4, h float8, i varchar, j bytea, k char, l varchar) with (appendonly=true, orientation=parquet);");
    util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tdthawq_register_data_types.paq testhawqregister_testdatatypes.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
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

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tanhawq_register_data_types.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    util.execute("create table hawqregister(a bool, b int2, c int2, d int4, e int8, f date, g float4, h float8, i varchar, j bytea, k char, l varchar) with (appendonly=true, orientation=parquet);");
    util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tanhawq_register_data_types.paq testhawqregister_testallnull.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
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

    auto cmd = hawq::test::stringFormat("hadoop fs -mkdir -p %s/hawq_register_test/t", getHdfsLocation().c_str());
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

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_test testhawqregister_testfiles.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
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

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/hawq_register_test_not_paq testhawqregister_testusage1notparquetfile.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
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

    /* register a parquet file to a row random table, should fail */
    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1npthawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    util.execute("create table hawqregister(i int);");
    util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1npthawq_register_hawq.paq testhawqregister_testusage1notparquettable.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 0);
    util.execute("drop table hawqregister;");

    /* register a parquet file to a row hash table, should fail */
    util.execute("create table hawqregister(i int) distributed by (i);");
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1npthawq_register_hawq.paq testhawqregister_testusage1notparquettable.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 0);
    util.execute("drop table hawqregister;");

    /* register a parquet file to a parquet hash table, should fail */
    util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet) distributed by (i);");
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1npthawq_register_hawq.paq testhawqregister_testusage1notparquettable.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 0);
    util.execute("drop table hawqregister;");

    cmd = hawq::test::stringFormat("hadoop fs -rm  %s/tu1npthawq_register_hawq.paq", getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));
}

TEST_F(TestHawqRegister, TestUsage1FileNotExist) {
    SQLUtility util;

    util.execute("create table hawqregister(i int);");
    util.query("select * from hawqregister;", 0);

    auto cmd = hawq::test::stringFormat("hawq register -d %s -f %shawq_register_file_not_exist testhawqregister_testusage1notparquettable.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 0);

    util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1NoTable) {
    SQLUtility util;
    string rootPath(util.getTestRootPath());
    string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
    string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1nthawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    /* register a parquet file to a table not exist, should fail */
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s testhawqregister_testusage1notable.hawqregister", HAWQ_DB, getHdfsLocation().c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
}

TEST_F(TestHawqRegister, TestUsage1NotHDFSPath) {
    SQLUtility util;
    string rootPath(util.getTestRootPath());
    string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
    string filePath = rootPath + relativePath;

    /* register a non-hdfs parquet file, should failed */
    util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
    util.query("select * from hawqregister;", 0);

    auto cmd = hawq::test::stringFormat("hawq register -d %s -f %s testhawqregister_testusage1nothdfspath.hawqregister", HAWQ_DB, filePath.c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 0);

    util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1EofSuccess) {
    SQLUtility util;
    string rootPath(util.getTestRootPath());
    string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
    string filePath = rootPath + relativePath;

    /* register a parquet file with eof=0, should success */
    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1eshawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
    util.query("select * from hawqregister;", 0); 

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1eshawq_register_hawq.paq testhawqregister_testusage1eofsuccess.hawqregister -e 0", HAWQ_DB, getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 0);

    /* register a parquet file with eof=filesize, should success */
    int size = getFileSize(filePath.c_str());
    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1eshawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1eshawq_register_hawq.paq testhawqregister_testusage1eofsuccess.hawqregister -e %d", HAWQ_DB, getHdfsLocation().c_str(), size);
    EXPECT_EQ(0, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 3);
    util.execute("drop table hawqregister;");

    /* register a parquet file with eof<filesize, should success */
    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1eshawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1eshawq_register_hawq.paq testhawqregister_testusage1eofsuccess.hawqregister -e %d", HAWQ_DB, getHdfsLocation().c_str(), size/2);
    EXPECT_EQ(0, Command::getCommandStatus(cmd));
    util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1EofFailure) {
    SQLUtility util;
    string rootPath(util.getTestRootPath());
    string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
    string filePath = rootPath + relativePath;

    auto cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/tu1efhawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    /* register a parquet file with eof=-1, should success */
    util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");
    util.query("select * from hawqregister;", 0);

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1efhawq_register_hawq.paq testhawqregister_testusage1eoffailure.hawqregister -e -1", HAWQ_DB, getHdfsLocation().c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 0);

    /* register a parquet file with eof > filesize, should success */
    int size = getFileSize(filePath.c_str());
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1efhawq_register_hawq.paq testhawqregister_testusage1eoffailure.hawqregister -e %d", HAWQ_DB, getHdfsLocation().c_str(), size+1);
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
    util.query("select * from hawqregister;", 0);

    /* register a parquet file with eof=float, should success */
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/tu1efhawq_register_hawq.paq testhawqregister_testusage1eoffailure.hawqregister -e 11.1", HAWQ_DB, getHdfsLocation().c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));
    util.execute("drop table hawqregister;");
}

TEST_F(TestHawqRegister, TestUsage1FolderFailure) {
    SQLUtility util;
    string folderName = "usage1_folder_tmp";
    string folderNameNotExist = "usage1_folder_tmpNotExist";
    string rootPath(util.getTestRootPath());
    string relativePath("/ManagementTool/test_hawq_register_hawq.paq");
    string filePath = rootPath + relativePath;
    string relativePath2("/ManagementTool/files_incomplete.yml");
    string filePath2 = rootPath + relativePath2;

    auto cmd = hawq::test::stringFormat("hdfs dfs -mkdir %s/%s", getHdfsLocation().c_str(), folderName.c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    util.execute("create table hawqregister(i int) with (appendonly=true, orientation=parquet);");

    /* register a empty folder, should success */
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/%s testhawqregister_testUsage1folderfailure.hawqregister", HAWQ_DB, getHdfsLocation().c_str(), folderName.c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    /* register a not exist folder, should fail */
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/%s testhawqregister_testUsage1folderfailure.hawqregister", HAWQ_DB, getHdfsLocation().c_str(), folderNameNotExist.c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));

    /* register a folder with eof, should fail */
    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/%s testhawqregister_testUsage1folderfailure.hawqregister -e 100", HAWQ_DB, getHdfsLocation().c_str(), folderName.c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));

    /* register a folder containing parquet and non-parquet files, should fail */
    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/%s/tu1ffhawq_register_hawq.paq", filePath.c_str(), getHdfsLocation().c_str(), folderName.c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    cmd = hawq::test::stringFormat("hadoop fs -put -f %s %s/%s/", filePath2.c_str(), getHdfsLocation().c_str(), folderName.c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));

    cmd = hawq::test::stringFormat("hawq register -d %s -f %s/%s testhawqregister_testUsage1folderfailure.hawqregister", HAWQ_DB, getHdfsLocation().c_str(), folderName.c_str());
    EXPECT_EQ(1, Command::getCommandStatus(cmd));

    util.execute("drop table hawqregister;");
    cmd = hawq::test::stringFormat("hdfs dfs -rm -r %s/%s", getHdfsLocation().c_str(), folderName.c_str());
    EXPECT_EQ(0, Command::getCommandStatus(cmd));
}
