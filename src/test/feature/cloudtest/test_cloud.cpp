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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <fstream>

#include "lib/command.h"
#include "lib/sql_util_parallel.h"
#include "lib/xml_parser.h"
#include "test_cloud.h"

#define CREATE_ROLE "CREATE ROLE"
#define ALTER_ROLE "ALTER ROLE"
#define DROP_ROLE "DROP ROLE"

using hawq::test::Command;

TEST_F(TestCloud, Permissions) {
  ASSERT_TRUE(testLogin(getUserName(), HAWQ_PASSWORD))
      << "Connect Error: Can't login as " + getUserName();
  execSql("drop role if exists lava;");
  execSql("drop role if exists user1;");
  execSql("drop role if exists user2;");
  execSql(getHost2(), "drop role if exists user2;");

  std::string result;
  result = execSql("create role lava password \'lava00000000\';");
  const std::string lava_pwd("lava00000000");
  ASSERT_TRUE(checkOut(result, CREATE_ROLE))
      << "create role lava failure: " + result;
  EXPECT_FALSE(testLogin("lava", lava_pwd))
      << "Error: lava can login before grant login permission";

  result = execSql("alter role lava login;");
  ASSERT_TRUE(checkOut(result, ALTER_ROLE))
      << "grant lava login permission failure: " + result;
  ASSERT_TRUE(testLogin("lava", lava_pwd))
      << "Error: lava can't login after grant login permission";

  result = execSql("alter role lava Superuser;");
  ASSERT_TRUE(checkOut(result, ALTER_ROLE))
      << "grant lava Superuser permision failure: " + result;
  result = execSql("alter role lava Createrole;");
  ASSERT_TRUE(checkOut(result, ALTER_ROLE))
      << "grant lava Createrole permision failure: " + result;
  result = execSql("lava", lava_pwd,
                   "create role user1 login password \'user100000000\';");
  ASSERT_TRUE((checkOut(result, CREATE_ROLE)))
      << "lava create user1 failure: " + result;
  ASSERT_TRUE(testLogin("user1", "user100000000")) << "login as user1 failure";

  result = execSql("lava", lava_pwd,
                   "alter role user1 password \'newPwd00000000\';");
  const std::string user_newPwd = "newPwd00000000";
  ASSERT_TRUE(checkOut(result, ALTER_ROLE))
      << "lava reset user1's password failure";
  EXPECT_FALSE(testLogin("user1", "user100000000"))
      << "user1 can login after reset password";
  EXPECT_TRUE(testLogin("user1", user_newPwd))
      << "user1 user new password can't login after reset password";

  result = execSql("user1", user_newPwd,
                   "create role user2 password \'user200000000\'");
  EXPECT_FALSE(checkOut(result, CREATE_ROLE))
      << "user1 can create user2 before get creatrole permissions: " + result;
  result = execSql("lava", lava_pwd, "alter role user1 createrole;");
  ASSERT_TRUE(checkOut(result, ALTER_ROLE))
      << "lava grant user1 createrole permissions failure: " + result;
  result = execSql("user1", user_newPwd,
                   "create role user2 password \'user200000000\'");
  ASSERT_TRUE(checkOut(result, CREATE_ROLE))
      << "user1 can't create user2 after get creatrole permissions: " + result;

  result = execSql("lava", lava_pwd, "alter role user1 nocreaterole;");
  ASSERT_TRUE(checkOut(result, ALTER_ROLE))
      << "lava can't revoke createrole from user1:" << result;
  result = execSql("user1", user_newPwd, "drop role user2;");
  ASSERT_FALSE(checkOut(result, DROP_ROLE))
      << "user can drop user2 after revoked createrole permissions: " + result;

  result = execSql("lava", lava_pwd, "drop role user2;");
  ASSERT_TRUE(checkOut(result, DROP_ROLE)) << "lava can't drop user2";

  result = execSql(getHost2(), "create role user2 password \'user200000000\';");
  ASSERT_TRUE(checkOut(result, CREATE_ROLE))
      << "lavaIntern can't create user2 in cluster2: " + result;

  result = execSql("lava", lava_pwd, "alter role user2 login;");
  ASSERT_TRUE(checkOut(result, ALTER_ROLE))
      << "lava can't grant login permission in cluster1 to user2: " + result;

  ASSERT_TRUE(testLogin("user2", "user200000000"))
      << "login as user2 in cluster1 failure";
  result = execSql(getHost2(), "drop role user2;");
  ASSERT_TRUE(checkOut(result, DROP_ROLE))
      << "drop user2 failure in cluster2: " + result;
  ASSERT_FALSE(testLogin("user2", "user200000000"))
      << "user2 can login in cluster1 after drop user2 in cluster1";
}

TEST_F(TestCloud, Normal) {
  Command::getCommandStatus("mkdir -p " + getTestRootPath() +
                            "/cloudtest/out/normal");
  configConnection();
  execSQLfile("cloudtest/sql/normal/before_normal.sql",
              "cloudtest/out/normal/before_normal.out");
  execSQLfileandCheck("cloudtest/sql/normal/all.sql",
                      "cloudtest/out/normal/all.out");
}

#define PROCESS_NUM 10
#define ITERATION_NUM 1
TEST_F(TestCloud, Parallel) {
  std::string result;
  Command::getCommandStatus("mkdir -p " + getTestRootPath() +
                            "/cloudtest/out/parallel");
  Command::getCommandStatus("rm -rf " + getTestRootPath() +
                            "/cloudtest/out/parallel/round*");
  configConnection();
  execSQLfile("cloudtest/sql/parallel/before_parallel.sql",
              "cloudtest/out/parallel/before_parallel.out");
  for (int i = 0; i < ITERATION_NUM; i++) {
    std::string outpath =
        "cloudtest/out/parallel/round" + std::to_string(i) + "/";
    Command::getCommandStatus("mkdir -p " + getTestRootPath() + "/" + outpath);
    runParallelSQLFile("cloudtest/sql/parallel/parallel.sql",
                       outpath + "parallel.out", PROCESS_NUM);
  }

  std::string outputFile;
  for (int i = 0; i < ITERATION_NUM; ++i)
    for (int j = 0; j < PROCESS_NUM; ++j) {
      outputFile = "cloudtest/out/parallel/round" + std::to_string(i) + "/" +
                   "parallel_process_" + std::to_string(j) + ".out";
      bool result = hawq::test::SQLUtility::checkPatternInFile(
          outputFile, ": ERROR:|FATAL|PANIC");
      EXPECT_TRUE(result) << outputFile << " find : ERROR:|FATAL|PANIC";
    }
}

TestCloud::TestCloud()
    : testRootPath(hawq::test::SQLUtility::getTestRootPath()), usexml(false) {
  setConfigXml("cloudtest/test_cloud.xml");
}

bool TestCloud::setConfigXml(const std::string &xmlfile) {
  if (xmlfile.empty()) {
    std::cout << "xml file should not be empty when you call setConfigXml";
    return false;
  } else {
    std::string xmlabsfile = testRootPath + "/" + xmlfile;
    if (!std::ifstream(xmlabsfile)) {
      std::cout << xmlabsfile << " doesn't exist";
      return false;
    }
    try {
      std::unique_ptr<hawq::test::XmlConfig> xmlconf;
      xmlconf.reset(new hawq::test::XmlConfig(xmlabsfile));
      xmlconf->parse();
      hostName = xmlconf->getString("cloud_host");
      cluster2host = xmlconf->getString("cloud_host2");
      port = xmlconf->getString("cloud_port", "5432");
      databaseName = xmlconf->getString("cloud_database", "postgres");
      userName = xmlconf->getString("cloud_user");
      usexml = true;
      xmlconf.release();
      return true;
    } catch (...) {
      std::cout << "Parse Xml file" << xmlabsfile << "error";
      return false;
    }
  }
}

void TestCloud::configConnection() {
  conn.reset(new hawq::test::PSQL(databaseName, hostName, port, userName,
                                  HAWQ_PASSWORD));
}

std::string TestCloud::getTestRootPath() const { return this->testRootPath; }

void TestCloud::execSQLfile(const std::string &sqlFile,
                            const std::string &outFile) {
  std::string sqlFileAbsPath = testRootPath + "/" + sqlFile;
  std::string outFileAbsPath = testRootPath + "/" + outFile;
  conn->setOutputFile(outFileAbsPath);
  EXPECT_EQ(0, conn->runSQLFile(sqlFileAbsPath, "").getLastStatus());
  conn->resetOutput();
}

void TestCloud::execSQLfileandCheck(const std::string &sqlFile,
                                    const std::string &outFile) {
  execSQLfile(sqlFile, outFile);
  bool result = hawq::test::SQLUtility::checkPatternInFile(
      outFile, ": ERROR:|FATAL|PANIC");
  EXPECT_TRUE(result) << outFile << " find ERROR|FATAL|PANIC";
}

bool TestCloud::testLogin(const std::string &host, const std::string &portStr,
                          const std::string &database, const std::string &user,
                          const std::string &pwd) const {
  std::string loginCmd = "psql -h " + host + " -p " + portStr + " -d " +
                         database + " -U " + user + " --c '\\q'";
  std::string result = Command::getCommandOutput("export PGPASSWORD=\'" + pwd +
                                                 "\' && " + loginCmd);
  return result.empty();
}

bool TestCloud::testLogin(const std::string &user,
                          const std::string &pwd) const {
  return testLogin(hostName, port, databaseName, user, pwd);
}

bool TestCloud::testLogin(const std::string &pwd) const {
  return testLogin(hostName, port, databaseName, userName, pwd);
}

std::string TestCloud::execSql(const std::string &host,
                               const std::string &sql) {
  return execSql(host, port, databaseName, userName, HAWQ_PASSWORD, sql);
}

std::string TestCloud::execSql(const std::string &sql) {
  return execSql(userName, HAWQ_PASSWORD, sql);
}

std::string TestCloud::execSql(const std::string &user, const std::string &pwd,
                               const std::string &sql) {
  return execSql(hostName, port, databaseName, user, pwd, sql);
}

std::string TestCloud::execSql(const std::string &host,
                               const std::string &portStr,
                               const std::string &database,
                               const std::string &user, const std::string &pwd,
                               const std::string &sql) {
  std::string sqlCmd = "echo \"" + sql + "\" | psql -h " + host + " -p " +
                       portStr + " -d " + database + " -U " + user;
  std::string result = Command::getCommandOutput("export PGPASSWORD=\'" + pwd +
                                                 "\' && " + sqlCmd);
  return result;
}

bool TestCloud::checkOut(const std::string &out, const std::string &pattern) {
  return (out.find("ERROR:") != std::string::npos)
             ? false
             : (out.find(pattern) != std::string::npos);
}

void TestCloud::runParallelSQLFile(const std::string sqlFile,
                                   const std::string outputFile, int processnum,
                                   const std::string ansFile) {
  hawq::test::SQLUtility sqlUtil(this->databaseName, this->hostName, this->port,
                                 this->userName);
  sqlUtil.runParallelSQLFile(sqlFile, outputFile, processnum, ansFile);
}
