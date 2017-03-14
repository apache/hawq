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

#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <vector>

#include "sql_util.h"
#include "string_util.h"

#ifdef __linux__
#include <limits.h>
#include <unistd.h>
#elif __APPLE__
#include <libproc.h>
#endif

using std::string;

namespace hawq {
namespace test {

SQLUtility::SQLUtility(SQLUtilityMode mode)
    : testRootPath(getTestRootPath()),
      test_info(::testing::UnitTest::GetInstance()->current_test_info()) {
  auto getConnection = [&] () {
    string user = HAWQ_USER;
    if(user.empty()) {
      struct passwd *pw;
      uid_t uid = geteuid();
      pw = getpwuid(uid);
      user.assign(pw->pw_name);
    }
    conn.reset(new hawq::test::PSQL(HAWQ_DB, HAWQ_HOST, HAWQ_PORT, user, HAWQ_PASSWORD));
  };
  getConnection();

  if (mode == MODE_SCHEMA) {
    schemaName = string(test_info->test_case_name()) + "_" + test_info->name();
    databaseName = HAWQ_DB;
    exec("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
    exec("CREATE SCHEMA " + schemaName);
    sql_util_mode = MODE_SCHEMA;
  } else {
    schemaName = HAWQ_DEFAULT_SCHEMA;
    databaseName = "db_" + string(test_info->test_case_name()) + "_" + test_info->name();
    std::transform(databaseName.begin(), databaseName.end(), databaseName.begin(), ::tolower);
    exec("DROP DATABASE IF EXISTS " + databaseName);
    exec("CREATE DATABASE " + databaseName);
    sql_util_mode = MODE_DATABASE;
  }
}

SQLUtility::~SQLUtility() {
  if (!test_info->result()->Failed()) {
    if (schemaName != HAWQ_DEFAULT_SCHEMA) {
      exec("DROP SCHEMA " + schemaName + " CASCADE");
    }

    if (sql_util_mode ==  MODE_DATABASE) {
      exec("DROP DATABASE " + databaseName);
    }
  }
}

std::string SQLUtility::getDbName() {
    return databaseName;
}

std::string SQLUtility::getSchemaName() {
    return schemaName;
}

void SQLUtility::exec(const string &sql) {
  EXPECT_EQ(0, (conn->runSQLCommand(sql)).getLastStatus())
      << conn->getLastResult();
}

string SQLUtility::execute(const string &sql, bool check) {
  conn->runSQLCommand("SET SEARCH_PATH=" + schemaName + ";" + sql);
  EXPECT_NE(conn.get(), nullptr);
  if (check) {
    EXPECT_EQ(0, conn->getLastStatus()) << conn->getLastResult();
    return "";
  }
  return conn.get()->getLastResult();
}

void SQLUtility::executeExpectErrorMsgStartWith(const std::string &sql,
                                                const std::string &errmsg) {
  std::string errout = execute(sql, false);
  EXPECT_STREQ(errmsg.c_str(), errout.substr(0, errmsg.size()).c_str());
}

void SQLUtility::executeIgnore(const string &sql) {
  conn->runSQLCommand("SET SEARCH_PATH=" + schemaName + ";" + sql);
  EXPECT_NE(conn.get(), nullptr);
}

void SQLUtility::query(const string &sql, int expectNum) {
  const hawq::test::PSQLQueryResult &result = executeQuery(sql);
  ASSERT_FALSE(result.isError()) << result.getErrorMessage();
  EXPECT_EQ(expectNum, result.rowCount());
}

void SQLUtility::query(const string &sql, const string &expectStr) {
  const hawq::test::PSQLQueryResult &result = executeQuery(sql);
  ASSERT_FALSE(result.isError()) << result.getErrorMessage();
  std::vector<std::vector<string> > resultString = result.getRows();
  string resultStr;
  for (auto row : result.getRows()) {
    for (auto column : row) resultStr += column + "|";
    resultStr += "\n";
  }
  EXPECT_EQ(expectStr, resultStr);
}

void SQLUtility::execSQLFile(const string &sqlFile,
                             const string &ansFile,
                             const string &initFile,
							 bool usingDefaultSchema,
							 bool printTupleOnly) {
	printf("dd2d%s\n",schemaName.c_str());
  FilePath fp;

  // do precheck for sqlFile & ansFile
  if (hawq::test::startsWith(sqlFile, "/") ||
      hawq::test::startsWith(ansFile, "/"))
    ASSERT_TRUE(false) << "For sqlFile and ansFile, relative path to feature "
                          "test root dir is needed";
  string ansFileAbsPath = testRootPath + "/" + ansFile;
  if (!std::ifstream(ansFileAbsPath))
    ASSERT_TRUE(false) << ansFileAbsPath << " doesn't exist";
  fp = splitFilePath(ansFileAbsPath);
  // double check to avoid empty fileBaseName
  if (fp.fileBaseName.empty())
    ASSERT_TRUE(false) << ansFileAbsPath << " is invalid";

  // generate new sql file with set search_path added at the begining
  const string newSqlFile = generateSQLFile(sqlFile, usingDefaultSchema);

  // outFile is located in the same folder with ansFile
  string outFileAbsPath = fp.path + "/" + fp.fileBaseName + ".out";
  conn->setOutputFile(outFileAbsPath);
  EXPECT_EQ(0, conn->runSQLFile(newSqlFile, printTupleOnly).getLastStatus());
  conn->resetOutput();

  // initFile if any
  string initFileAbsPath;
  if (!initFile.empty()) {
    initFileAbsPath = testRootPath + "/" + initFile;
    if (!std::ifstream(initFileAbsPath))
      ASSERT_TRUE(false) << initFileAbsPath << " doesn't exist";
    fp = splitFilePath(initFileAbsPath);
    // double check to avoid empty fileBaseName
    if (fp.fileBaseName.empty())
      ASSERT_TRUE(false) << initFileAbsPath << " is invalid";
  } else {
    initFileAbsPath = "";
  }

  string globalInitFileAbsPath;
  globalInitFileAbsPath = testRootPath + "/lib/global_init_file";

  bool is_sql_ans_diff = conn->checkDiff(ansFileAbsPath, outFileAbsPath, true, globalInitFileAbsPath, initFileAbsPath);
  EXPECT_FALSE(is_sql_ans_diff);
  if (is_sql_ans_diff == false) {
    // no diff, continue to delete the generated sql file
    if (remove(newSqlFile.c_str()))
      ASSERT_TRUE(false) << "Error deleting file " << newSqlFile;
  } else {
    EXPECT_FALSE(true);
  }
}

bool SQLUtility::execSQLFile(const string &sqlFile) {
  // do precheck for sqlFile
  if (hawq::test::startsWith(sqlFile, "/"))
    return false;

  // double check to avoid empty fileBaseName
  FilePath fp = splitFilePath(sqlFile);
  if (fp.fileBaseName.empty())
    return false;
  // outFile is located in the same folder with ansFile
  string outFileAbsPath = "/tmp/" + fp.fileBaseName + ".out";

  // generate new sql file with set search_path added at the begining
  const string newSqlFile = generateSQLFile(sqlFile, false);

  // run sql file and store its result in output file
  conn->setOutputFile(outFileAbsPath);
  return conn->runSQLFile(newSqlFile).getLastStatus() == 0 ? true : false;
}

const string SQLUtility::generateSQLFile(const string &sqlFile, bool usingDefaultSchema) {
  const string originSqlFile = testRootPath + "/" + sqlFile;
  const string newSqlFile = "/tmp/" + string(test_info->test_case_name()) + "_" + test_info->name() + ".sql";
  std::fstream in;
  in.open(originSqlFile, std::ios::in);
  if (!in.is_open()) {
    EXPECT_TRUE(false) << "Error opening file " << originSqlFile;
  }
  std::fstream out;
  out.open(newSqlFile, std::ios::out);
  if (!out.is_open()) {
    EXPECT_TRUE(false) << "Error opening file " << newSqlFile;
  }
  out << "-- start_ignore" << std::endl;
  printf("dd2d%s\n",schemaName.c_str());
  if (!usingDefaultSchema) {
	  printf("ddd%s\n",schemaName.c_str());
	  out << "SET SEARCH_PATH=" + schemaName + ";" << std::endl;
  }
  if (sql_util_mode ==  MODE_DATABASE) {
    out << "\\c " << databaseName << std::endl;
  }
  out << "-- end_ignore" << std::endl;
  string line;
  while (getline(in, line)) {
    out << line << std::endl;
  }
  in.close();
  out.close();
  return newSqlFile;
}

const hawq::test::PSQLQueryResult &SQLUtility::executeQuery(const string &sql) {
  const hawq::test::PSQLQueryResult &result =
      conn->getQueryResult("SET SEARCH_PATH=" + schemaName + ";" + sql);
  return result;
}

const hawq::test::PSQL *SQLUtility::getPSQL() const { return conn.get(); }

string SQLUtility::getTestRootPath() const {
  string result;
#ifdef __linux__
  char pathbuf[PATH_MAX];
  ssize_t count = readlink("/proc/self/exe", pathbuf, PATH_MAX);
  if (count <= 0)
    EXPECT_TRUE(false) << "readlink /proc/self/exe error: " << strerror(errno);
  result = string(pathbuf, count);
#elif __APPLE__
  int ret;
  pid_t pid;
  char pathbuf[PROC_PIDPATHINFO_MAXSIZE];

  pid = getpid();
  ret = proc_pidpath(pid, pathbuf, sizeof(pathbuf));
  if (ret <= 0)
    EXPECT_TRUE(false) << "PID " << pid << ": proc_pidpath () "
                       << strerror(errno);
  result = string(pathbuf);
#endif
  return splitFilePath(result).path;
}

void SQLUtility::setGUCValue(const std::string &guc, const std::string &value) {
  string sql = "set " + guc + " = " + value;
  execute(sql, true);
}

std::string SQLUtility::getGUCValue(const std::string &guc) {
  string sql = "show " + guc;
  const hawq::test::PSQLQueryResult &result = executeQuery(sql);
  EXPECT_EQ(result.rowCount(), 1);
  std::vector<std::string> row = result.getRows()[0];
  return row[0];
}

std::string SQLUtility::getQueryResult(const std::string &query) {
  const hawq::test::PSQLQueryResult &result = executeQuery(query);
  EXPECT_LE(result.rowCount(), 1);
  std::string value;
  if (result.rowCount() == 1)
  {
    value = result.getRows()[0][0];
  }

  return value;
}

std::string SQLUtility::getQueryResultSetString(const std::string &query) {
  const hawq::test::PSQLQueryResult &result = executeQuery(query);
  std::vector<std::vector<string> > resultString = result.getRows();
  string resultStr;
  for (auto row : result.getRows()) {
    for (auto column : row) resultStr += column + "|";
    resultStr += "\n";
  }
  return resultStr;
}

FilePath SQLUtility::splitFilePath(const string &filePath) const {
  FilePath fp;
  size_t found1 = filePath.find_last_of("/");
  size_t found2 = filePath.find_last_of(".");
  fp.path = filePath.substr(0, found1);
  fp.fileBaseName = filePath.substr(found1 + 1, found2 - found1 - 1);
  fp.fileSuffix = filePath.substr(found2 + 1, filePath.length() - found2 - 1);
  return fp;
}

} // namespace test
} // namespace hawq
