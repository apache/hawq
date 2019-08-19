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

#include "gtest/gtest.h"

#include "lib/file_replace.h"
#include "lib/hdfs_config.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"

using hawq::test::FileReplace;
using hawq::test::HdfsConfig;
using hawq::test::SQLUtility;

class TestExtOrc : public ::testing::Test {
 public:
  const std::string initFile = "ExternalSource/sql/init_file";
  TestExtOrc() {}
  ~TestExtOrc() {}

 public:
  bool checkHDFSCommand() {
    if (system("which hdfs > /dev/null 2>&1")) return false;
    return true;
  }

  std::string generateUrl(SQLUtility &util, const std::string &dir) {
    if (!checkHDFSCommand()) return "";
    HdfsConfig hc;
    std::string hdfsPrefix;
    hc.getNamenodeHost(hdfsPrefix);
    std::string url =
        "hdfs://" + hdfsPrefix + TestExtOrc::getHAWQDefaultPath(util) + dir;
    std::string cmd = "hdfs dfs -rm -R " + url;
    std::cout << "prepare hdfs : " << cmd << std::endl;
    std::string result;
    hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
    return url;
  }

  std::string getHAWQDefaultPath(SQLUtility &util) {
    std::string url = util.getGUCValue("hawq_dfs_url");
    std::size_t found = url.find("/");
    std::string result = url.substr(found);
    if (result[result.size() - 1] != '/') {
      result += "/";
    }
    return result;
  }
};

TEST_F(TestExtOrc, TestNormalPath) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::cout << basepath << "\n";
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test_normalpath";
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  if (status) std::cout << "delete the hdfs";

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_normalpath.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_normalpath.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_normalpath.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_normalpath.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_normalpath.sql",
                   "ExternalSource/ans/exttable_extorc_normalpath.ans");

  std::string dbname = util.getDbName();
  std::string schemaname = util.getSchemaName();
  std::string testsql;
  std::string tablespacename;
  testsql =
      "select pg_tablespace.spcname from pg_tablespace,pg_database "
      "where pg_tablespace.oid=pg_database.dat2tablespace and "
      "pg_database.datname='" +
      dbname + "';";
  tablespacename = util.getQueryResult(testsql);
  cmd = "hdfs dfs -ls " + basepath + tablespacename + "/" + dbname + "/" +
        schemaname + "/normal_orctable1";
  hawq::test::toLower(cmd);
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  EXPECT_TRUE(status) << "Internal table can not find path \n";

  util.execute("drop table normal_orctable1;");

  cmd = "hdfs dfs -ls " + basepath + tablespacename + "/" + dbname + "/" +
        schemaname + "/normal_orctable1";
  hawq::test::toLower(cmd);
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  EXPECT_FALSE(status) << "Internal table still exists after dropped \n";

  std::cout << hdfs_prefix << "\n";
  std::cout << basepath << "\n";
  cmd = "hdfs dfs -ls hdfs://" + hdfs_prefix + basepath +
        "exttable_extorc_test_normalpath/normal_orctable1_e";
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  EXPECT_TRUE(status) << "External table can not find path \n";

  util.execute("drop external table normal_orctable1_e;");

  cmd = "hdfs dfs -ls hdfs://" + hdfs_prefix + basepath +
        "exttable_extorc_test_normalpath/normal_orctable1_e";
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  EXPECT_TRUE(status) << "External table still exists after dropped \n";

  cmd = "hdfs dfs -rm -R " + basepath + "exttable_extorc_test_normalpath";
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
}

TEST_F(TestExtOrc, TestNegativePath) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test_negativepath";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::cout << hdfs_prefix << basepath << "\n";
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_negativepath.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_negativepath.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_negativepath.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_negativepath.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_negativepath.sql",
                   "ExternalSource/ans/exttable_extorc_negativepath.ans",
                   initFile);

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestSameName) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/same_name_test";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::cout << hdfs_prefix << basepath << "\n";
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_same_name_test.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_same_name_test.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_same_name_test.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_same_name_test.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_same_name_test.sql",
                   "ExternalSource/ans/exttable_extorc_same_name_test.ans",
                   initFile);

  // cleanup in hdfs
  cmd = "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/same_name_test";
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestEncoding) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/encoding";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_encoding.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_encoding.sql", test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_encoding.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_encoding.ans", test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists te;");
  util.executeIgnore("drop external table if exists twe_e, tre_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_encoding.sql",
                   "ExternalSource/ans/exttable_extorc_encoding.ans");

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestDatatypeInteger) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/datatype_integer";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_integer.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_integer.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_integer.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_integer.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists ti;");
  util.executeIgnore("drop external table if exists twi_e, tri_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_datatype_integer.sql",
                   "ExternalSource/ans/exttable_extorc_datatype_integer.ans");

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestDatatypeBool) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/datatype_bool";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_bool.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_bool.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_bool.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_bool.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists tb;");
  util.executeIgnore("drop external table if exists twb_e, trb_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_datatype_bool.sql",
                   "ExternalSource/ans/exttable_extorc_datatype_bool.ans",
                   initFile);

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestDatatypeDouble) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/datatype_double";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_double.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_double.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_double.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_double.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists td;");
  util.executeIgnore("drop external table if exists twd_e, trd_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_datatype_double.sql",
                   "ExternalSource/ans/exttable_extorc_datatype_double.ans");

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestDatatypeDecimal) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/datatype_decimal";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_decimal.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_decimal.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_decimal.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_decimal.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists td, tn;");
  util.executeIgnore(
      "drop external table if exists twd_e, trd_e, twn_e, trn_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_datatype_decimal.sql",
                   "ExternalSource/ans/exttable_extorc_datatype_decimal.ans");

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestDatatypeVarchar) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/datatype_varchar";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_varchar.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_varchar.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_varchar.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_varchar.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists tv;");
  util.executeIgnore("drop external table if exists twv_e, trv_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_datatype_varchar.sql",
                   "ExternalSource/ans/exttable_extorc_datatype_varchar.ans");

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestDatatypeChar) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/datatype_char";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_char.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_char.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_char.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_char.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists tc, tcn;");
  util.executeIgnore(
      "drop external table if exists twc_e, trc_e, twcn_e, trcn_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_datatype_char.sql",
                   "ExternalSource/ans/exttable_extorc_datatype_char.ans");

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestDatatypeDateTime) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/datatype_datetime";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_datetime.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_datetime.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_datetime.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_datetime.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists td;");
  util.executeIgnore("drop external table if exists twd_e, trd_e;");

  util.executeIgnore("drop table if exists tt, ttntz, tttz;");
  util.executeIgnore(
      "drop external table if exists twt_e, trt_e, twtntz_e, trtntz_e, "
      "twttz_e, trttz_e;");

  util.executeIgnore("drop table if exists tts, ttsntz, ttstz;");
  util.executeIgnore(
      "drop external table if exists twts_e, trts_e, twtsntz_e, trtsntz_e, "
      "twtstz_e, trtstz_e;");

  util.executeIgnore("drop table if exists tit;");
  util.executeIgnore("drop external table if exists twit_e, trit_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_datatype_datetime.sql",
                   "ExternalSource/ans/exttable_extorc_datatype_datetime.ans");

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, TestDatatypeMoney) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  bool status;

  // Prepare hdfs
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_test/datatype_money";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);

  // prepare testing sql and ans files
  std::string hdfs_prefix;
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_money.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_datatype_money.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_money.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_datatype_money.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);

  // prepare in database
  util.executeIgnore("drop table if exists tm;");
  util.executeIgnore("drop external table if exists twm_e, trm_e;");

  // run test
  util.execSQLFile("ExternalSource/sql/exttable_extorc_datatype_money.sql",
                   "ExternalSource/ans/exttable_extorc_datatype_money.ans");

  // cleanup in hdfs
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  ASSERT_TRUE(status);
}

TEST_F(TestExtOrc, DISABLED_TestTruncate) {
  SQLUtility util;
  HdfsConfig hc;
  std::string result;
  std::string sql, sqlout;
  std::string hdfs_prefix;
  bool status;
  std::string basepath = util.getHdfsPath();
  std::string cmd =
      "hdfs dfs -rm -R " + basepath + "exttable_extorc_testtruncate";
  std::cout << "prepare hdfs : " << cmd << std::endl;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  hc.getNamenodeHost(hdfs_prefix);
  FileReplace frep;
  std::string test_root(util.getTestRootPath());
  std::unordered_map<std::string, std::string> strs_src_dst;
  strs_src_dst["@@host@@"] = hdfs_prefix;
  strs_src_dst["@@path@@"] = basepath;
  std::string sql_src(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_testtruncate.sql.source",
      test_root.c_str()));
  std::string sql_dst(hawq::test::stringFormat(
      "%s/ExternalSource/sql/exttable_extorc_testtruncate.sql",
      test_root.c_str()));
  frep.replace(sql_src, sql_dst, strs_src_dst);

  std::string ans_src(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_testtruncate.ans.source",
      test_root.c_str()));
  std::string ans_dst(hawq::test::stringFormat(
      "%s/ExternalSource/ans/exttable_extorc_testtruncate.ans",
      test_root.c_str()));
  frep.replace(ans_src, ans_dst, strs_src_dst);
  util.execSQLFile("ExternalSource/sql/exttable_extorc_testtruncate.sql",
                   "ExternalSource/ans/exttable_extorc_testtruncate.ans");
  cmd = "hdfs dfs -ls " + basepath + "exttable_extorc_testtruncate";
  std::cout << cmd << std::endl;
  int num = 0;
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  size_t pos = 0;
  while ((pos = result.find(basepath, pos)) != std::string::npos) {
    ++num;
    ++pos;
  };
  EXPECT_EQ(1, num);
  sql = "insert into exttable_extorc_testtruncate values(1);";
  sqlout = util.execute(sql, true);
  sql = "drop external table exttable_extorc_testtruncate;";
  sqlout = util.execute(sql, true);
  status = hc.runCommand(cmd, hc.getHdfsUser(), result, HDFS_COMMAND);
  pos = 0;
  num = 0;
  while ((pos = result.find(basepath, pos)) != std::string::npos) {
    ++num;
    ++pos;
  };
  EXPECT_EQ(2, num);
}

TEST_F(TestExtOrc, BoolTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_BoolTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t10");
  util.execute(
      "create writable external table t10 (p bool) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t10 values('yes'),('n'),(null),('0'),(true)");
  util.query("select * from t10 where p='f' or p is true",
             "t|\nf|\nf|\nt|\n");
  util.query("select * from t10 where not p is unknown",
             "t|\nf|\nf|\nt|\n");
  util.query("select * from t10 where 't' > p",
             "f|\nf|\n");
  util.query("select * from t10 where p != 't'",
             "f|\nf|\n");
}

TEST_F(TestExtOrc, DateTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_DateTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t11");
  util.execute(
      "create writable external table t11 (p date) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute(
      "insert into t11 values('1999-01-08'),('January 8, "
      "1999'),('01/02/"
      "03'),('1999-Jan-08'),('Jan-08-1999'),('08-Jan-1999'),('"
      "19990108'),('990108'),('1999.008'),('J2451187'),('0099-01-08 BC')");
  util.query(
      "set datestyle to 'ISO, MDY'; select * from t11 "
      "where p != '2001-1-1'",
      "1999-01-08|\n1999-01-08|\n2003-01-02|\n1999-01-08|"
      "\n1999-01-08|\n1999-01-08|\n1999-01-08|\n1999-01-08|\n1999-01-08|"
      "\n1999-01-08|\n0099-01-08 BC|\n");
  util.query(
      "set datestyle to 'ISO, MDY'; select * from t11 "
      "where p < '2000-1-1'",
      "1999-01-08|\n1999-01-08|\n1999-01-08|"
      "\n1999-01-08|\n1999-01-08|\n1999-01-08|\n1999-01-08|\n1999-01-08|"
      "\n1999-01-08|\n0099-01-08 BC|\n");
  util.query(
      "select p - '2002-01-02' from t11 where p > "
      "'2000-1-1'",
      "365|\n");
  util.query(
      "set datestyle to 'ISO, MDY'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "0099-01-08 BC|\n");
  util.query(
      "set datestyle to 'ISO, DMY'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "0099-01-08 BC|\n");
  util.query(
      "set datestyle to 'ISO, YMD'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "0099-01-08 BC|\n");
  util.query(
      "set datestyle to 'Postgres, MDY'; select * from "
      "t11 where p = '0099-1-8 BC'",
      "01-08-0099 BC|\n");
  util.query(
      "set datestyle to 'Postgres, DMY'; select * from "
      "t11 where p = '0099-1-8 BC'",
      "08-01-0099 BC|\n");
  util.query(
      "set datestyle to 'Postgres, YMD'; select * from "
      "t11 where p = '0099-1-8 BC'",
      "01-08-0099 BC|\n");
  util.query(
      "set datestyle to 'SQL, MDY'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "01/08/0099 BC|\n");
  util.query(
      "set datestyle to 'SQL, DMY'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "08/01/0099 BC|\n");
  util.query(
      "set datestyle to 'SQL, YMD'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "01/08/0099 BC|\n");
  util.query(
      "set datestyle to 'German, MDY'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "08.01.0099 BC|\n");
  util.query(
      "set datestyle to 'German, DMY'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "08.01.0099 BC|\n");
  util.query(
      "set datestyle to 'German, YMD'; select * from t11 "
      "where p = '0099-1-8 BC'",
      "08.01.0099 BC|\n");
}

TEST_F(TestExtOrc, TimeTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_TimeTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t12");
  util.execute(
      "create writable external table t12 (p time(2)) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute(
      "insert into t12 "
      "values('04:05:06.789'),('04:05:06'),('04:05'),('040506'),('04:05 "
      "AM'),('04:05 "
      "PM'),('04:05:06.789-8'),('04:05:06-08:00'),('04:05-08:00'),('040506-08')"
      ",('04:05:06 PST'),('2003-04-12 04:05:06 America/New_York')");
  util.query(
      "select * from t12 where p = '04:05:06'",
      "04:05:06|\n04:05:06|\n04:05:06|\n04:05:06|\n04:05:06|\n04:05:06|\n");
  util.query("select * from t12 where p = '04:05:06.79'",
             "04:05:06.79|\n04:05:06.79|\n");
  util.query("select * from t12 where p > '04:05:06'",
             "04:05:06.79|\n16:05:00|\n04:05:06.79|\n");
  util.query("select * from t12 where p < '04:05:06'",
             "04:05:00|\n04:05:00|\n04:05:00|\n");
}

TEST_F(TestExtOrc, TimestampTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_TimestampTypeTest");
  std::cout << url << std::endl;
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t13");
  util.execute(
      "create writable external table t13 (p timestamp) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute(
      "insert into t13 values('1999-01-08 04:05:06'), ('January 8 04:05:06 "
      "1999 PST'), ('1999-01-08 04:05:06.01 BC')");
  util.query(
      "set datestyle to 'ISO, YMD'; select * from t13 "
      "where p > '1999-01-08 04:05:06 BC'",
      "1999-01-08 04:05:06|\n1999-01-08 04:05:06|\n1999-01-08 04:05:06.01 "
      "BC|\n");
  util.query(
      "set datestyle to 'ISO, YMD'; select * from t13 "
      "where p < '1999-01-08 04:05:06'",
      "1999-01-08 04:05:06.01 BC|\n");
  util.query(
      "set datestyle to 'ISO, MDY'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "1999-01-08 04:05:06|\n1999-01-08 04:05:06|\n");
  util.query(
      "set datestyle to 'ISO, DMY'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "1999-01-08 04:05:06|\n1999-01-08 04:05:06|\n");
  util.query(
      "set datestyle to 'ISO, YMD'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "1999-01-08 04:05:06|\n1999-01-08 04:05:06|\n");
  util.query(
      "set datestyle to 'Postgres, MDY'; select * from "
      "t13 where p = '1999-01-08 04:05:06'",
      "Fri Jan 08 04:05:06 1999|\nFri Jan 08 04:05:06 1999|\n");
  util.query(
      "set datestyle to 'Postgres, DMY'; select * from "
      "t13 where p = '1999-01-08 04:05:06'",
      "Fri 08 Jan 04:05:06 1999|\nFri 08 Jan 04:05:06 1999|\n");
  util.query(
      "set datestyle to 'Postgres, YMD'; select * from "
      "t13 where p = '1999-01-08 04:05:06'",
      "Fri Jan 08 04:05:06 1999|\nFri Jan 08 04:05:06 1999|\n");
  util.query(
      "set datestyle to 'SQL, MDY'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "01/08/1999 04:05:06|\n01/08/1999 04:05:06|\n");
  util.query(
      "set datestyle to 'SQL, DMY'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "08/01/1999 04:05:06|\n08/01/1999 04:05:06|\n");
  util.query(
      "set datestyle to 'SQL, YMD'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "01/08/1999 04:05:06|\n01/08/1999 04:05:06|\n");
  util.query(
      "set datestyle to 'German, MDY'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "08.01.1999 04:05:06|\n08.01.1999 04:05:06|\n");
  util.query(
      "set datestyle to 'German, DMY'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "08.01.1999 04:05:06|\n08.01.1999 04:05:06|\n");
  util.query(
      "set datestyle to 'German, YMD'; select * from t13 "
      "where p = '1999-01-08 04:05:06'",
      "08.01.1999 04:05:06|\n08.01.1999 04:05:06|\n");
}

TEST_F(TestExtOrc, BinaryTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_BinaryTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t14");
  util.execute(
      "create writable external table t14 (arr bytea) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t14 values('haha'), ('233'), ('666'), ('ok')");
  util.query("select * from t14",
             "haha|\n233|\n666|\nok|\n");
  util.query("select octet_length(arr) from t14",
             "4|\n3|\n3|\n2|\n");
  util.query("select * from t14 where arr > '233'",
             "haha|\n666|\nok|\n");
  util.query("select * from t14 where arr = '233'",
             "233|\n");
  util.query("select * from t14 where arr < '666'",
             "233|\n");
}

TEST_F(TestExtOrc, StringTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_StringTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t15");
  util.execute(
      "create writable external table "
      "t15(b \"char\", c char, charn char(5), varchar varchar, varcharn "
      "varchar(10), text text) "
      "LOCATION (\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t15 values('0', 'a ', 'b      ', 'c', 'd', 'e');");
  util.query("select * from t15", "0|a|b    |c|d|e|\n");
}

TEST_F(TestExtOrc, DISABLED_SmallIntArrayTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_SmallIntArrayTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t16");
  util.execute(
      "create writable external table t16 (p int2[10]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t16 values('{1,2,3}'), ('{NULL, 2}'), (NULL);");
  util.query("select * from t16",
             "{1,2,3}|\n{NULL,2}|\n|\n");
}

TEST_F(TestExtOrc, DISABLED_IntArrayTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_IntArrayTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t17");
  util.execute(
      "create writable external table t17 (p int4[10]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t17 values('{1,2,3}'), ('{NULL, 2}'), (NULL);");
  util.query("select * from t17",
             "{1,2,3}|\n{NULL,2}|\n|\n");
}

TEST_F(TestExtOrc, DISABLED_BigIntArrayTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_BigIntArrayTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t18");
  util.execute(
      "create writable external table t18 (p int8[10]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t18 values('{1,2,3}'), ('{NULL, 2}'), (NULL);");
  util.query("select * from t18",
             "{1,2,3}|\n{NULL,2}|\n|\n");
}

TEST_F(TestExtOrc, DISABLED_FloatArrayTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_FloatArrayTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t19");
  util.execute(
      "create writable external table t19 (p float4[10]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t19 values('{1,2,3}'), ('{NULL, 2}'), (NULL);");
  util.query("select * from t19",
             "{1,2,3}|\n{NULL,2}|\n|\n");
}

TEST_F(TestExtOrc, DISABLED_DoubleArrayTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_DoubleArrayTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t20");
  util.execute(
      "create writable external table t20 (p float8[10]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t20 values('{1,2,3}'), ('{NULL, 2}'), (NULL);");
  util.query("select * from t20",
             "{1,2,3}|\n{NULL,2}|\n|\n");
}

TEST_F(TestExtOrc, DISABLED_SmallIntArrayTypeWithSortedAnsTest) {
  SQLUtility util;
  std::string url =
      generateUrl(util, "TestExtOrc_SmallIntArrayWithSortedAnsTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t22");
  util.execute(
      "create writable external table t22 (i int, p int2[]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t22 values(1), (2);");
  util.execute(
      "insert into t22 values(11, '{1,2,NULL,4,5,NULL}'),(NULL, NULL),(31, "
      "'{1,2,3}');");
  util.execute(
      "insert into t22 values(12, NULL),(13, "
      "'{NULL,1,2,3}');");
  util.query("select * from t22 order by i",
             "1||\n2||\n11|{1,2,NULL,4,5,NULL}|\n12||\n13|{NULL,1,2,3}|\n31|{1,"
             "2,3}|\n||\n");
}

TEST_F(TestExtOrc, DISABLED_IntArrayTypeWithSortedAnsTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_IntArrayWithSortedAnsTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t23");
  util.execute(
      "create writable external table t23 (i int, p int4[]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t23 values(1), (2);");
  util.execute(
      "insert into t23 values(11, '{1,2,NULL,4,5,NULL}'),(NULL, NULL),(31, "
      "'{1,2,3}');");
  util.execute(
      "insert into t23 values(12, NULL),(13, "
      "'{NULL,1,2,3}');");
  util.query("select * from t23 order by i",
             "1||\n2||\n11|{1,2,NULL,4,5,NULL}|\n12||\n13|{NULL,1,2,3}|\n31|{1,"
             "2,3}|\n||\n");
}

TEST_F(TestExtOrc, DISABLED_BigIntArrayTypeWithSortedAnsTest) {
  SQLUtility util;
  std::string url =
      generateUrl(util, "TestExtOrc_BigIntArrayWithSortedAnsTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t24");
  util.execute(
      "create writable external table t24 (i int, p int8[]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t24 values(1), (2);");
  util.execute(
      "insert into t24 values(11, '{1,2,NULL,4,5,NULL}'),(NULL, NULL),(31, "
      "'{1,2,3}');");
  util.execute(
      "insert into t24 values(12, NULL),(13, "
      "'{NULL,1,2,3}');");
  util.query("select * from t24 order by i",
             "1||\n2||\n11|{1,2,NULL,4,5,NULL}|\n12||\n13|{NULL,1,2,3}|\n31|{1,"
             "2,3}|\n||\n");
}

TEST_F(TestExtOrc, DISABLED_FloatArrayTypeWithSortedAnsTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_FloatArrayWithSortedAnsTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t25");
  util.execute(
      "create writable external table t25 (i int, p float4[]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t25 values(1), (2);");
  util.execute(
      "insert into t25 values(11, '{1,2,NULL,4,5,NULL}'),(NULL, NULL),(31, "
      "'{1,2,3}');");
  util.execute(
      "insert into t25 values(12, NULL),(13, "
      "'{NULL,1,2,3}');");
  util.query("select * from t25 order by i",
             "1||\n2||\n11|{1,2,NULL,4,5,NULL}|\n12||\n13|{NULL,1,2,3}|\n31|{1,"
             "2,3}|\n||\n");
}

TEST_F(TestExtOrc, DISABLED_DoubleArrayTypeWithSortedAnsTest) {
  SQLUtility util;
  std::string url =
      generateUrl(util, "TestExtOrc_DoubleArrayWithSortedAnsTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t26");
  util.execute(
      "create writable external table t26 (i int, p float8[]) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute("insert into t26 values(1), (2);");
  util.execute(
      "insert into t26 values(11, '{1,2,NULL,4,5,NULL}'),(NULL, NULL),(31, "
      "'{1,2,3}');");
  util.execute(
      "insert into t26 values(12, NULL),(13, "
      "'{NULL,1,2,3}');");
  util.query("select * from t26 order by i",
             "1||\n2||\n11|{1,2,NULL,4,5,NULL}|\n12||\n13|{NULL,1,2,3}|\n31|{1,"
             "2,3}|\n||\n");
}

TEST_F(TestExtOrc, DISABLED_MixedTypeWithArrayTypeTest) {
  SQLUtility util;
  std::string url = generateUrl(util, "TestExtOrc_MixedTypeWithArrayTypeTest");
  ASSERT_FALSE(url.empty());
  util.execute("drop external table if exists t27");
  util.execute(
      "create writable external table t27 (p float8[], i int, s text, t "
      "timestamp) LOCATION "
      "(\'" +
      url + "\') FORMAT 'orc'");
  util.execute(
      "insert into t27 values('{1,2,NULL,4,5,NULL}', 11, 'abc', '2018-3-20 "
      "11:51:14.233'),(NULL, NULL, NULL, NULL),('{1,2,3}', 31, '', '2018-3-20 "
      "11:51:14');");
  util.execute(
      "insert into t27 values(NULL, 12, 'f', NULL),('{NULL,1,2,3}', 13, NULL, "
      "'2018-3-20 11:51:15'), ('{}', NULL, 'eee', '2018-3-20 11:51:14.233');");
  util.query("select * from t27 order by i, s",
             "{1,2,NULL,4,5,NULL}|11|abc|Tue Mar 20 11:51:14.233 "
             "2018|\n|12|f||\n{NULL,1,2,3}|13||Tue Mar 20 11:51:15 "
             "2018|\n{1,2,3}|31||Tue Mar 20 11:51:14 2018|\n{}||eee|Tue Mar 20 "
             "11:51:14.233 2018|\n||||\n");
  util.query(
      "select * from t27 where s is not null "
      "order by t,i ",
      "{1,2,3}|31||Tue Mar 20 11:51:14 2018|\n{1,2,NULL,4,5,NULL}|11|abc|Tue "
      "Mar 20 11:51:14.233 2018|\n{}||eee|Tue Mar 20 11:51:14.233 "
      "2018|\n|12|f||\n");
}

TEST_F(TestExtOrc, DISABLED_EuclideanMetricFloat8Array) {
  SQLUtility util;
  util.execute("drop  table if exists t28");
  util.execute("create table t28 (p float8[])  FORMAT 'orc'");
  util.execute("insert into t28 values('{1,2,3}'), ('{1,2}');");
  util.query(
      "select euclidean_metric_float8array(p, "
      "array[1,2,3]) from t28;",
      "0|\n3|\n");
  util.query(
      "select euclidean_metric_float8array(p, "
      "array[1,2]) from t28;",
      "3|\n0|\n");
}

TEST_F(TestExtOrc, DISABLED_EuclideanMetricFloat4Array) {
  SQLUtility util;
  util.execute("drop  table if exists t29");
  util.execute("create table t29 (p float4[])  FORMAT 'orc'");
  util.execute("insert into t29 values('{1,2,3}'), ('{1,2}');");
  util.query(
      "select euclidean_metric_float4array(p, "
      "array[1,2,3]) from t29;",
      "0|\n3|\n");
  util.query(
      "select euclidean_metric_float4array(p, "
      "array[1,2]) from t29;",
      "3|\n0|\n");
}

TEST_F(TestExtOrc, DISABLED_CosineDistanceFloat8Array) {
  SQLUtility util;
  util.execute("drop  table if exists t30");
  util.execute("create table t30 (p float8[])  FORMAT 'orc'");
  util.execute("insert into t30 values('{1,2,0}'), ('{1,2}');");
  util.query(
      "select cosine_distance_float8array(p, "
      "array[1,2,0]) from t30;",
      "1|\n1|\n");
  util.query(
      "select cosine_distance_float8array(p, "
      "array[1,2]) from t30;",
      "1|\n1|\n");
}

TEST_F(TestExtOrc, DISABLED_CosineDistanceFloat4Array) {
  SQLUtility util;
  util.execute("drop  table if exists t31");
  util.execute("create table t31 (p float4[])  FORMAT 'orc'");
  util.execute("insert into t31 values('{1,2,0}'), ('{1,2}');");
  util.query(
      "select cosine_distance_float4array(p, "
      "array[1,2,0]) from t31;",
      "1|\n1|\n");
  util.query(
      "select cosine_distance_float4array(p, "
      "array[1,2]) from t31;",
      "1|\n1|\n");
}
