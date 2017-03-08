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

#ifndef HAWQ_SRC_TEST_FEATURE_PSQL_H_
#define HAWQ_SRC_TEST_FEATURE_PSQL_H_

#include <string>
#include <vector>

#include "command.h"
#include "libpq-fe.h"

namespace hawq {
namespace test {

class PSQLQueryResult {
 public:
  PSQLQueryResult() {}

  void savePGResult(const PGresult* res);
  void setErrorMessage(const std::string& errmsg);
  const std::string& getErrorMessage() const;
  bool isError() const;

  const std::vector<std::vector<std::string> >& getRows() const;
  const std::vector<std::string>& getFields() const;

  const std::vector<std::string>& getRow(int ri) const;
  const std::string& getData(int ri, int ci) const;
  std::string getData(int ri, const std::string& ck) const;
  const std::string& getFieldName(int ci) const;

  int rowCount() const;
  int fieldCount() const;

  void reset();

 private:
  PSQLQueryResult(const PSQLQueryResult&);
  const PSQLQueryResult& operator=(const PSQLQueryResult&);

  std::string _errmsg;
  std::vector<std::vector<std::string> > _rows;
  std::vector<std::string> _fields;
};

class PSQL {
 public:
  PSQL(const std::string& db, const std::string& host = "localhost",
       const std::string& port = "5432", const std::string& user = "gpadmin",
       const std::string& password = "")
      : _dbname(db), _host(host), _port(port),
      _user(user), _password(password) {}
  virtual ~PSQL(){};

  PSQL& runSQLCommand(const std::string& sql_cmd);
  PSQL& runSQLFile(const std::string& sql_file, bool printTupleOnly = false);
  const PSQLQueryResult& getQueryResult(const std::string& sql);

  PSQL& setHost(const std::string& host);
  PSQL& setPort(const std::string& port);
  PSQL& setUser(const std::string& username);
  PSQL& setPassword(const std::string& password);
  PSQL& setOutputFile(const std::string& out);
  std::string getConnectionString() const;

  int getLastStatus() const;
  const std::string& getLastResult() const;

  static bool checkDiff(const std::string& expect_file,
                        const std::string& result_file,
                        bool save_diff = true,
                        const std::string& global_init_file = "",
                        const std::string& init_file = "");

  void resetOutput();

 private:
  PSQL(const PSQL&);
  const PSQL& operator=(const PSQL&);

  const std::string _getPSQLBaseCommand() const;
  const std::string _getPSQLQueryCommand(const std::string& query) const;
  const std::string _getPSQLFileCommand(const std::string& file, bool printTupleOnly = false) const;

  std::string _dbname;
  std::string _host;
  std::string _port;
  std::string _user;
  std::string _password;
  std::string _output_file;
  PSQLQueryResult _result;

  int _last_status;
  std::string _last_result;
};

} // namespace test
} // namespace hawq

#endif
