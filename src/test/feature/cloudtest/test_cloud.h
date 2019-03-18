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

#ifndef TEST_CLOUD_H
#define TEST_CLOUD_H

#include <string>

#include "gtest/gtest.h"
#include "lib/psql.h"

class TestCloud : public ::testing::Test {
 public:
  TestCloud();
  ~TestCloud() {}

  // config connection param use member hostName,port,databaseName,userName
  void configConnection();

  // get some value from xmlfile
  // @xmlfile the abs path of xml file
  // @return config done status
  bool setConfigXml(const std::string &xmlfile);

  // @return test root path
  std::string getTestRootPath() const;

  // test login user member hostName,port databaseName
  // @user user Name
  // @pwd  login password
  // @return login status
  bool testLogin(const std::string &user, const std::string &pwd) const;

  // test login use member hostName,port,databaseName,userName
  // @pwd login password
  // @return login status
  bool testLogin(const std::string &pwd) const;

  // test login
  // @host host name
  // @portStr which port
  // @database database name to connect
  // @user user name
  // @pwd login password
  // @return login status
  bool testLogin(const std::string &host, const std::string &portStr,
                 const std::string &database, const std::string &user,
                 const std::string &pwd) const;

  // execute sql file and check outfile
  // @sqlfile sql file relative path
  // @outfile out file relative path
  void execSQLfileandCheck(const std::string &sqlfile,
                           const std::string &outfile);

  // execute sql file
  // @sqlfile sql file relative path
  // @outfile out file relative path
  void execSQLfile(const std::string &sqlfile, const std::string &outfile);

  // execute sql in host,other param use member
  // @host host name
  // @sql sql query
  // @return out of sql
  std::string execSql(const std::string &host, const std::string &sql);

  // execute sql
  // @sql sql query
  // @return out of sql
  std::string execSql(const std::string &sql);

  // execute sql query by user
  // @user who execute sql
  // @pwd user's password
  // @sql sql query
  // @return out of execute sql
  std::string execSql(const std::string &user, const std::string &pwd,
                      const std::string &sql);

  // execute sql query
  // @host name
  // @portStr port
  // @database database name
  // @user who execute sql
  // @pwd user's password
  // @sql sql query
  // @return out of execute sql
  std::string execSql(const std::string &host, const std::string &portStr,
                      const std::string &database, const std::string &user,
                      const std::string &pwd, const std::string &sql);
  // check out use pattern
  // @out out of sql
  // @pattern out need have string
  // @return out is satisfactory
  bool checkOut(const std::string &out, const std::string &pattern = "");

  // run parallel sql file
  // @sqlFile to be run sql file
  // @outputFile out file of run sql file
  // @processnum number of process
  // @ansFile expect out file to compare to out file
  void runParallelSQLFile(const std::string sqlFile,
                          const std::string outputFile, int processnum,
                          const std::string ansFile = "");

  // @return another host
  std::string getHost2() const { return this->cluster2host; }

  // @return port
  std::string getPort() const { return this->port; }

  // @return user name
  std::string getUserName() const { return this->userName; }

 private:
  std::unique_ptr<hawq::test::PSQL> conn;
  std::string testRootPath;

  std::string hostName;
  std::string cluster2host;
  std::string port;
  std::string databaseName;
  std::string userName;
  bool usexml;
};

#endif
