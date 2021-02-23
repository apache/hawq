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

#ifndef HAWQ_SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_SQL_UTIL_H_

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "psql.h"

#define HAWQ_DB (getenv("PGDATABASE") ? getenv("PGDATABASE") : "postgres")
#define HAWQ_HOST (getenv("PGHOST") ? getenv("PGHOST") : "localhost")
#define HAWQ_PORT (getenv("PGPORT") ? getenv("PGPORT") : "5432")
#define HAWQ_USER (getenv("PGUSER") ? getenv("PGUSER") : getenv("USER"))
#define HAWQ_PASSWORD (getenv("PGPASSWORD") ? getenv("PGPASSWORD") : "")
#define HAWQ_DEFAULT_SCHEMA ("public")
#define RANGER_HOST (getenv("RANGERHOST") ? getenv("RANGERHOST") : "localhost")
#define KUBENET_MASTER \
  (getenv("KUBENET_MASTER") ? getenv("KUBENET_MASTER") : "localhost")
#define HIVE_HOST (getenv("HIVEHOST") ? getenv("HIVEHOST") : "localhost")
#define HIVE_PORT (getenv("HIVEPORT") ? getenv("HIVEPORT") : "9083")
#define TIME_LOG() hawq::test::SQLUtility::time_log()
#define EXPECT_TRUE_RET_IF_FALSE(expression, errorMessage) \
  {                                                        \
    bool ret;                                              \
    EXPECT_TRUE(ret = (expression)) << errorMessage;       \
    if (!ret) return false;                                \
  }

namespace hawq {
namespace test {

struct FilePath {
  std::string path;
  std::string fileBaseName;
  std::string fileSuffix;
};

enum SQLUtilityMode {
  MODE_SCHEMA,
  MODE_DATABASE,
  MODE_MAX_NUM,
  MODE_MADLIB,
  MODE_SCHEMA_NODROP
};

#define COST_TIME(START_TIMEB, END_TIMEB, UINT32_COST_TIME, CODES) \
  timeb START_TIMEB, END_TIMEB;                                    \
  ftime(&START_TIMEB);                                             \
  CODES                                                            \
  ftime(&END_TIMEB);                                               \
  UINT32_COST_TIME =                                               \
      (uint32_t)(((END_TIMEB.timezone - END_TIMEB.timezone) * 60 + \
                  (END_TIMEB.time - START_TIMEB.time)) *           \
                     1000 +                                        \
                 (END_TIMEB.millitm - START_TIMEB.millitm));

class SQLUtility {
 public:
  SQLUtility(SQLUtilityMode mode = MODE_SCHEMA);

  SQLUtility(const std::string &db, const std::string &host = "localhost",
             const std::string &port = "5432",
             const std::string &user = "gpadmin");

  ~SQLUtility();
  // Get the local system time
  // @return string of local system time
  static std::string getStrCurTime();

  // print log with current local time
  // @return std::cout
  static std::ostream &time_log();

  // Get the test Host
  // @return string of the test Host
  std::string getHost();

  // Get the test database name
  // @return string of the test database name
  std::string getDbName();

  // Get the test schema name
  // @return string of the test schema name
  std::string getSchemaName();

  // Get the test database version
  // @return string of test database version
  std::string getVersion();

  // Set a new test SchemaName
  void setSchemaName(const std::string &name, bool isSwitch = false);

  // test the connection is ok or not
  bool testConnection() { return conn->testConnection(); }

  // return test info of gtest
  const ::testing::TestInfo *const getTestInfo() { return this->test_info; }

  // Execute sql command
  // @param sql The given sql command
  // @param check true(default) if expected correctly executing, false otherwise
  // @return error or notice message if check is false, else return empty
  std::string execute(const std::string &sql, bool check = true);

  // Execute sql command
  // @param sql The given sql command
  // @return true if executed sql successfully, else return false
  bool executeSql(const std::string &sql);

  // Execute sql command and ignore the behavior of its running status
  // @param sql The given sql command
  // @return void
  void executeIgnore(const std::string &sql);

  // Execute query command and check the rowCount
  // @param sql The given query command
  // @param expectNum The expected rowCount
  // @return void
  void query(const std::string &sql, int expectNum);

  // Execute query command and check query result
  // @param sql The given query command
  // @param expectStr The given query result
  // @return void
  void query(const std::string &sql, const std::string &expectStr);

  // Execute sql file and diff with ans file
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @param ansFile The given ansFile which is relative path to test root dir
  // @param initFile The given initFile (used by gpdiff.pl) which is relative
  // path to test root dir
  // @return void
  void execSQLFile(const std::string &sqlFile, const std::string &ansFile,
                   const std::string &initFile = "",
                   bool usingDefaultSchema = false,
                   bool printTupleOnly = false);

  // Execute sql file and check its return status
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @return true if the sql file is executed successfully, false otherwise
  bool execSQLFile(const std::string &sqlFile);

  // Execute sql file and check its return status
  // @param sqlFile The given sqlFile which is absolute path
  // @return true if the sql file is executed successfully, false otherwise
  bool execAbsSQLFile(const std::string &sqlFile);

  // Execute sql file and check its output file
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @param outFile The given outFile which is relatice path to test root dir
  // @param pattern provides the keywords such as ERROR to check in output file
  // @return true if the sql file is executed successfully, false otherwise
  void execSQLFileandCheck(const std::string &sqlFile,
                           const std::string &outFile,
                           const std::string &pattern = "",
                           const std::string &sqloptions = "");

  // Execute sql file and diff with ans file
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @param outputFile The given output file which is relative path to test root
  // dir
  // @param ansFile The given ansFile which is relative path to test root dir
  // @param psqlOptions The psql options such as -v TABLENAME="test"
  // @param initFile The given initFile (used by gpdiff.pl) which is relative
  // path to test root dir
  // @return void
  void execSpecificSQLFile(
      const std::string &sqlFile, const std::string &outputFile,
      const std::string &ansFile, const std::string &psqlOptions,
      const std::string &sqlFilePrefix, const std::string &initFile = "",
      bool usingDefaultSchema = false, bool printTupleOnly = false);

  bool execSpecificSQLFile(uint32_t &costTime, const std::string &sqlFile,
                           const std::string &outputFile,
                           const std::string &ansFile,
                           const std::string &psqlOptions,
                           const std::string &sqlFilePrefix,
                           const std::string &initFile = "",
                           bool usingDefaultSchema = false,
                           bool printTupleOnly = false);

  // Execute sql file and check its return status
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @param outputFile The given output file which is relative path to test root
  // dir
  // @param psqlOptions The psql options such as -v TABLENAME="test"
  // @return true if the sql file is executed successfully, false otherwise
  bool execSpecificSQLFile(const std::string &sqlFile,
                           const std::string &outputFile,
                           const std::string &psqlOptions,
                           const std::string &sqlFilePrefix);

  bool execSpecificSQLFile(uint32_t &costTime, const std::string &sqlFile,
                           const std::string &outputFile,
                           const std::string &psqlOptions,
                           const std::string &sqlFilePrefix);

  // Execute sql file with append string in the head and check its return status
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @param outputFile The given output file which is relative path to test root
  // dir
  // @param psqlOptions The psql options such as -v TABLENAME="test"
  // @param appendString The append string such as \timing  explain analyze
  // @return true if the sql file is executed successfully, false otherwise
  bool execAppendSQLFile(const std::string &sqlFile,
                         const std::string &outputFile,
                         const std::string &psqlOptions,
                         const std::string &sqlFilePrefix,
                         const std::string &appendString);

  bool execAppendSQLFile(uint32_t &costTime, const std::string &sqlFile,
                         const std::string &outputFile,
                         const std::string &psqlOptions,
                         const std::string &sqlFilePrefix,
                         const std::string &appendString);
  // Execute sql file and diff with ans file
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @param outputFile The given output file which is relative path to test root
  // dir
  // @param ansFile The given ansFile which is relative path to test root dir
  // @param psqlOptions The psql options such as -v TABLENAME="test"
  // @param appendString The append string such as \timing  explain analyze
  // @param initFile The given initFile (used by gpdiff.pl) which is relative
  // path to test root dir
  // @param sortoutfile new executor do not support orderby sometimes check
  // outfile need to sort it before
  // @return void
  void execAppendSQLFile(
      const std::string &sqlFile, const std::string &outputFile,
      const std::string &ansFile, const std::string &psqlOptions,
      const std::string &sqlFilePrefix, const std::string &appendString,
      const std::string &initFile = "", bool usingDefaultSchema = false,
      bool printTupleOnly = false, bool sortoutfile = false);

  bool execAppendSQLFile(uint32_t &costTime, const std::string &sqlFile,
                         const std::string &outputFile,
                         const std::string &ansFile,
                         const std::string &psqlOptions,
                         const std::string &sqlFilePrefix,
                         const std::string &appendString,
                         const std::string &initFile = "",
                         bool usingDefaultSchema = false,
                         bool printTupleOnly = false, bool sortoutfile = false);

  void runParallelSQLFile(const std::string sqlFile,
                          const std::string outputFile, int processnum,
                          const std::string ansFile = "",
                          bool usingDefaultSchema = false);

  void runParallelSQLFile(const std::string sqlFile,
                          const std::string outputFile, int processnum,
                          std::vector<std::string> &options,
                          const std::string ansFile = "",
                          bool usingDefaultSchema = false);

  // Return the number of error in the output file
  // @param filename or matched string such as path/*.out
  int32_t getErrorNumber(const std::string &outputFile);

  // check error in the file
  // @param outFile The given outFile which is relatice path to test root dir
  // @param pattern provides the keywords such as ERROR to check in output file
  // @return true if the sql file is executed successfully, false otherwis
  static bool checkPatternInFile(const std::string &outFile,
                                 const std::string &pattern);

  // Get PSQL connection: do not suggest to use
  // @return PSQL raw pointer
  const hawq::test::PSQL *getPSQL() const;

  // Get test root dir abs path
  // @return path string
  static std::string getTestRootPath();

  // Set GUC value
  void setGUCValue(const std::string &guc, const std::string &value);

  // Set Host of connection value
  void setHost(const std::string &host);

  // Set Database of connection value
  void setDatabase(const std::string &db);

  // Get GUC value
  std::string getGUCValue(const std::string &guc);

  // execute given query and return query result
  // @param query the given query
  // @return the query result
  std::string getQueryResult(const std::string &query, bool check = true);

  std::string getQueryResultSetString(const std::string &query);

  // execute expect error message
  // @param sql the given sql command
  // @param errmsg the expected sql error message
  // @return void
  void executeExpectErrorMsgStartWith(const std::string &sql,
                                      const std::string &errmsg);

  const hawq::test::PSQLQueryResult &executeQuery(const std::string &sql);

  // return hdfs path : hawq_default
  std::string getHdfsPath();

  // return hdfs namenode and port: localhost:8020
  std::string getHdfsNamenode();

  // return hdfs dfs url: localhost:8020/hawq_default
  std::string getHawqDfsURL();

  // return hdfs dfs host: localhost:8020
  std::string getHawqDfsHost();

  // return magma service url: hostname:50001
  std::string getHawqMagmaURL();
  void runParallelSQLFile_sqllist(std::vector<std::string> &sqlfile,
                                  std::vector<std::string> &ansfile);
  // return true if OS is Darwin
  bool isDarwin();

 private:
  std::unique_ptr<hawq::test::PSQL> getConnection();
  const std::string generateSQLFile(const std::string &sqlFile,
                                    bool usingDefaultSchema,
                                    const std::string &sqlFileSurfix = "",
                                    const std::string &appendString = "");

  bool __beforeBoolExecAppendSQLFile(const std::string &sqlFile,
                                     const std::string &outputFile,
                                     const std::string &sqlFilePrefix,
                                     const std::string &appendString,
                                     std::string &newSqlFile);

  void __beforeRunSqlfile(const std::string &sqlFile,
                          const std::string &outputFile,
                          const std::string &ansFile,
                          const std::string &sqlFilePrefix,
                          const std::string &appendString,
                          bool usingDefaultSchema, FilePath &fp,
                          std::string &ansFileAbsPath, std::string &newSqlFile,
                          std::string &outputAbsPath);

  bool __afterRunSqlfile(const std::string &initFile, bool sortoutfile,
                         FilePath &fp, std::string &ansFileAbsPath,
                         std::string &outputAbsPath, std::string &newSqlFile);

  bool __beforeExecSpecificSQLFile(const std::string &sqlFile,
                                   const std::string &outputFile,
                                   const std::string &sqlFilePrefix,
                                   std::string &newSqlFile,
                                   std::string &outputAbsPath);

  void __beforeExecSpecificSQLFile(
      const std::string &sqlFile, const std::string &outputFile,
      const std::string &ansFile, const std::string &sqlFilePrefix,
      bool usingDefaultSchema, FilePath &fp, std::string &ansFileAbsPath,
      std::string &newSqlFile, std::string &outputAbsPath);

  void exec(const std::string &sql);

  void execIgnore(const std::string &sql);

 public:
  static FilePath splitFilePath(const std::string &filePath);
  bool checkAnsFile(const std::string &ansFileAbsPath,
                    const std::string &outFileAbsPath,
                    const std::string &initFile, const std::string &newSqlFile,
                    FilePath &fp);

  std::string getConnectionString() { return conn->getConnectionString(); }

 protected:
  std::string testRootPath;
  const ::testing::TestInfo *const test_info;

 private:
  SQLUtilityMode sql_util_mode;
  std::unique_ptr<hawq::test::PSQL> conn;
  std::string databaseName;
  std::string schemaName;
  std::string savedGUCValue;

};  // class SQLUtility

}  // namespace test
}  // namespace hawq

#endif  // SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
