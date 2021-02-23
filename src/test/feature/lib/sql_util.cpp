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
#include <iomanip>
#include <regex>
#include <vector>

#include "hawq_config.h"
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
  auto getConnection = [&]() {
    string user = HAWQ_USER;
    if (user.empty()) {
      struct passwd *pw;
      uid_t uid = geteuid();
      pw = getpwuid(uid);
      user.assign(pw->pw_name);
    }
    conn.reset(new hawq::test::PSQL(HAWQ_DB, HAWQ_HOST, HAWQ_PORT, user,
                                    HAWQ_PASSWORD));
  };
  getConnection();

  if (mode == MODE_SCHEMA || mode == MODE_SCHEMA_NODROP ||
      mode == MODE_MADLIB) {
    schemaName = string(test_info->test_case_name()) + "_" + test_info->name();
    databaseName = HAWQ_DB;
    sql_util_mode = mode;
    if (mode == MODE_SCHEMA || mode == MODE_MADLIB) {
      exec("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
      exec("CREATE SCHEMA " + schemaName);
    } else {
      execIgnore("CREATE SCHEMA " + schemaName);
    }
  } else if (mode == MODE_MADLIB) {
    schemaName = string(test_info->test_case_name()) + "_" + test_info->name();
    databaseName = HAWQ_DB;
    exec("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
    exec("CREATE SCHEMA " + schemaName);

  } else {
    schemaName = HAWQ_DEFAULT_SCHEMA;
    databaseName =
        "db_" + string(test_info->test_case_name()) + "_" + test_info->name();
    std::transform(databaseName.begin(), databaseName.end(),
                   databaseName.begin(), ::tolower);
    exec("DROP DATABASE IF EXISTS " + databaseName);
    exec("CREATE DATABASE " + databaseName);
    sql_util_mode = MODE_DATABASE;
  }
}

SQLUtility::SQLUtility(const std::string &db, const std::string &host,
                       const std::string &port, const std::string &user)
    : testRootPath(getTestRootPath()),
      test_info(::testing::UnitTest::GetInstance()->current_test_info()) {
  conn.reset(new hawq::test::PSQL(db, host, port, user, HAWQ_PASSWORD));
  schemaName = HAWQ_DEFAULT_SCHEMA;
  databaseName = db;
  sql_util_mode = MODE_SCHEMA_NODROP;
}

SQLUtility::~SQLUtility() {
  if (!test_info->result()->Failed()) {
    //--------------------------------------------------------------------------
    // This is a temporary work around to sleep a short time window in order to
    // wait for the quit of query dispatcher processes. Because each query
    // dispatcher has one resource heart-beat thread to be joined before the
    // exit, in worst case, that thread will sleep 100ms and consequently check
    // the switch variable to complete the exiting logic. This may causes the
    // error reporting that the database is still accessed by other users, when
    // user drops database once finished using database.
    //
    // When we have that exit logic improved, we can remove this logic.
    //--------------------------------------------------------------------------

    usleep(200000);
    if (schemaName != HAWQ_DEFAULT_SCHEMA &&
        sql_util_mode != MODE_SCHEMA_NODROP) {
      exec("DROP SCHEMA " + schemaName + " CASCADE");
    }

    if (sql_util_mode == MODE_DATABASE) {
      exec("DROP DATABASE " + databaseName);
    }
  }
}

std::string SQLUtility::getStrCurTime() {
  time_t secs = time(NULL);
  struct tm *tm_cur = localtime(&secs);
  std::string str_time = std::to_string(tm_cur->tm_year + 1900) + "-" +
                         std::to_string(tm_cur->tm_mon + 1) + "-" +
                         std::to_string(tm_cur->tm_mday) + " " +
                         std::to_string(tm_cur->tm_hour) + ":" +
                         std::to_string(tm_cur->tm_min) + ":" +
                         std::to_string(tm_cur->tm_sec);
  return str_time;
}

std::ostream &SQLUtility::time_log() {
  std::cout.flags(std::ios::left);
  std::cout << std::setw(18) << SQLUtility::getStrCurTime() << " => ";
  return std::cout;
}

std::string SQLUtility::getHost() { return conn->getHost(); }

std::string SQLUtility::getDbName() { return databaseName; }

std::string SQLUtility::getSchemaName() { return schemaName; }

void SQLUtility::setSchemaName(const std::string &name, bool isSwitch) {
  schemaName = name;
  if (!isSwitch) {
    assert(name != HAWQ_DEFAULT_SCHEMA);
    if (sql_util_mode != MODE_SCHEMA_NODROP) {
      exec("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
      exec("CREATE SCHEMA " + schemaName);
    } else {
      execIgnore("CREATE SCHEMA " + schemaName);
    }
  }
}

std::string SQLUtility::getVersion() {
  return getQueryResult("select version();");
}

// Set Host of connection value
void SQLUtility::setHost(const std::string &host) { conn->setHost(host); }

void SQLUtility::setDatabase(const std::string &db) { conn->setDatabase(db); }

void SQLUtility::exec(const string &sql) {
  EXPECT_EQ(0, (conn->runSQLCommand(sql)).getLastStatus())
      << conn->getLastResult();
}

void SQLUtility::execIgnore(const string &sql) { conn->runSQLCommand(sql); }

string SQLUtility::execute(const string &sql, bool check) {
  conn->runSQLCommand("SET SEARCH_PATH=" + schemaName + ";" + savedGUCValue + sql);
  EXPECT_NE(conn.get(), nullptr);
  if (check) {
    EXPECT_EQ(0, conn->getLastStatus()) << sql << " " << conn->getLastResult();
    return "";
  }
  return conn.get()->getLastResult();
}

bool SQLUtility::executeSql(const std::string &sql) {
  conn->runSQLCommand("SET SEARCH_PATH=" + schemaName + ";" + savedGUCValue + sql);
  EXPECT_NE(conn.get(), nullptr);
  return !conn->getLastStatus();
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

bool SQLUtility::checkAnsFile(const string &ansFileAbsPath,
                              const string &outFileAbsPath,
                              const string &initFile, const string &newSqlFile,
                              FilePath &fp) {
  // initFile if any
  string initFileAbsPath;
  if (!initFile.empty()) {
    initFileAbsPath = testRootPath + "/" + initFile;
    if (!std::ifstream(initFileAbsPath)) {
      EXPECT_TRUE(false) << initFileAbsPath << " doesn't exist";
      return false;
    }
    fp = splitFilePath(initFileAbsPath);
    // double check to avoid empty fileBaseName
    if (fp.fileBaseName.empty()) {
      EXPECT_TRUE(false) << initFileAbsPath << " is invalid";
      return false;
    }
  } else {
    initFileAbsPath = "";
  }

  string globalInitFileAbsPath;
  globalInitFileAbsPath = testRootPath + "/lib/global_init_file";

  bool is_sql_ans_diff =
      conn->checkDiff(ansFileAbsPath, outFileAbsPath, true,
                      globalInitFileAbsPath, initFileAbsPath);
  if (is_sql_ans_diff == false) {
    // no diff, continue to delete the generated sql file
    if (!newSqlFile.empty() && remove(newSqlFile.c_str())) {
      EXPECT_TRUE(false) << "Error deleting file " << newSqlFile;
      return false;
    }
    return true;
  } else {
    EXPECT_FALSE(is_sql_ans_diff) << conn->getDifFilePath(outFileAbsPath);
    return false;
  }
}

void SQLUtility::execSQLFile(const string &sqlFile, const string &ansFile,
                             const string &initFile, bool usingDefaultSchema,
                             bool printTupleOnly) {
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
  checkAnsFile(ansFileAbsPath, outFileAbsPath, initFile, newSqlFile, fp);
}

void SQLUtility::execSQLFileandCheck(const string &sqlFile,
                                     const string &outFile,
                                     const std::string &pattern,
                                     const std::string &sqlOptions) {
  // do precheck for sqlFile
  if (hawq::test::startsWith(sqlFile, "/")) {
    ASSERT_TRUE(false)
        << "For sqlFile, relative path to feature test root dir is needed";
  }
  string sqlFileAbsPath = testRootPath + "/" + sqlFile;
  FilePath fp = splitFilePath(sqlFileAbsPath);
  string outFileAbsPath = testRootPath + "/" + outFile;

  if (fp.fileBaseName.empty()) ASSERT_TRUE(false) << sqlFile << " is invalid";

  // generate new sql file with set search_path added at the begining
  const string newSqlFile = generateSQLFile(sqlFile, false);
  conn->setOutputFile(outFileAbsPath);

  EXPECT_EQ(0, conn->runSQLFile(newSqlFile, sqlOptions).getLastStatus());
  conn->resetOutput();
  bool result = this->checkPatternInFile(outFile, pattern);
  ASSERT_TRUE(result) << outFile << " find " << pattern;
}

bool SQLUtility::checkPatternInFile(const std::string &outFile,
                                    const std::string &pattern) {
  if (hawq::test::startsWith(outFile, "/")) {
    std::cout << "ERROR : outfile format is error ";
    return false;
  }
  string outFileAbsPath = getTestRootPath() + "/" + outFile;
  std::ifstream fin(outFileAbsPath);
  std::string line;
  bool result = true;
  while (getline(fin, line) && result) {
    std::regex re(pattern, std::regex_constants::icase);
    if (std::regex_search(line, re)) {
      result = false;
      break;
    }
  }
  fin.close();
  return result;
}

bool SQLUtility::execAbsSQLFile(const string &sqlFile) {
  // double check to avoid empty fileBaseName
  FilePath fp = splitFilePath(sqlFile);
  if (fp.fileBaseName.empty()) return false;
  // outFile is located in the same folder with ansFile
  string outFileAbsPath = "/tmp/" + fp.fileBaseName + ".out";

  // run sql file and store its result in output file
  conn->setOutputFile(outFileAbsPath);
  return conn->runSQLFile(sqlFile).getLastStatus() == 0 ? true : false;
}

bool SQLUtility::execSQLFile(const string &sqlFile) {
  // do precheck for sqlFile
  if (hawq::test::startsWith(sqlFile, "/")) return false;

  // double check to avoid empty fileBaseName
  FilePath fp = splitFilePath(sqlFile);
  if (fp.fileBaseName.empty()) return false;
  // outFile is located in the same folder with ansFile
  string outFileAbsPath = "/tmp/" + fp.fileBaseName + ".out";

  // generate new sql file with set search_path added at the begining
  const string newSqlFile = generateSQLFile(sqlFile, false);

  // run sql file and store its result in output file
  conn->setOutputFile(outFileAbsPath);
  return conn->runSQLFile(newSqlFile).getLastStatus() == 0 ? true : false;
}

int32_t SQLUtility::getErrorNumber(const string &outputFile) {
  // do precheck for sqlFile
  string outputAbspath;
  if (hawq::test::startsWith(outputFile, "/"))
    outputAbspath = outputFile;
  else
    outputAbspath = testRootPath + "/" + outputFile;
  string cmdstr = "grep -i ERROR " + outputAbspath + " | wc -l";
  string output = Command::getCommandOutput(cmdstr);
  try {
    return std::stoi(output);
  } catch (...) {
    return -1;
  }
}

bool SQLUtility::__beforeExecSpecificSQLFile(const string &sqlFile,
                                             const string &outputFile,
                                             const string &sqlFilePrefix,
                                             string &newSqlFile,
                                             string &outputAbsPath) {
  // do precheck for sqlFile
  if (hawq::test::startsWith(sqlFile, "/")) return false;

  // double check to avoid empty fileBaseName
  FilePath fp = splitFilePath(sqlFile);
  if (fp.fileBaseName.empty()) return false;

  // generate new sql file with set search_path added at the begining
  newSqlFile = generateSQLFile(sqlFile, false, sqlFilePrefix);

  // outFile is located in the same folder with ansFile
  outputAbsPath = testRootPath + "/" + outputFile;

  return true;
}

bool SQLUtility::execSpecificSQLFile(const string &sqlFile,
                                     const string &outputFile,
                                     const string &psqlOptions,
                                     const string &sqlFilePrefix) {
  string newSqlFile, outputAbsPath;
  if (!__beforeExecSpecificSQLFile(sqlFile, outputFile, sqlFilePrefix,
                                   newSqlFile, outputAbsPath))
    return false;

  conn->setOutputFile(outputAbsPath);

  bool result = conn->runSQLFile(newSqlFile, psqlOptions).getLastStatus() == 0
                    ? true
                    : false;
  conn->resetOutput();
  return result;
}

bool SQLUtility::__beforeBoolExecAppendSQLFile(const string &sqlFile,
                                               const string &outputFile,
                                               const string &sqlFilePrefix,
                                               const string &appendString,
                                               string &newSqlFile) {
  // do precheck for sqlFile
  if (hawq::test::startsWith(sqlFile, "/")) return false;

  // double check to avoid empty fileBaseName
  FilePath fp = splitFilePath(sqlFile);
  if (fp.fileBaseName.empty()) return false;

  // generate new sql file with set search_path added at the begining
  newSqlFile = generateSQLFile(sqlFile, false, sqlFilePrefix, appendString);

  // outFile is located in the same folder with ansFile
  std::string outputAbsPath = testRootPath + "/" + outputFile;
  conn->setOutputFile(outputAbsPath);
  return true;
}

bool SQLUtility::execAppendSQLFile(const string &sqlFile,
                                   const string &outputFile,
                                   const string &psqlOptions,
                                   const string &sqlFilePrefix,
                                   const string &appendString) {
  std::string newSqlFile;
  if (!__beforeBoolExecAppendSQLFile(sqlFile, outputFile, sqlFilePrefix,
                                     appendString, newSqlFile))
    return false;

  bool result = conn->runSQLFile(newSqlFile, psqlOptions).getLastStatus() == 0
                    ? true
                    : false;
  conn->resetOutput();
  return result;
}

void SQLUtility::__beforeRunSqlfile(
    const string &sqlFile, const string &outputFile, const string &ansFile,
    const string &sqlFilePrefix, const string &appendString,
    bool usingDefaultSchema, FilePath &fp, std::string &ansFileAbsPath,
    std::string &newSqlFile, std::string &outputAbsPath) {
  // do precheck for sqlFile & ansFile
  if (hawq::test::startsWith(sqlFile, "/") ||
      hawq::test::startsWith(ansFile, "/"))
    ASSERT_TRUE(false) << "For sqlFile and ansFile, relative path to feature "
                          "test root dir is needed";
  ansFileAbsPath = testRootPath + "/" + ansFile;
  if (!std::ifstream(ansFileAbsPath))
    ASSERT_TRUE(false) << ansFileAbsPath << " doesn't exist";
  fp = splitFilePath(ansFileAbsPath);
  // double check to avoid empty fileBaseName
  if (fp.fileBaseName.empty())
    ASSERT_TRUE(false) << ansFileAbsPath << " is invalid";

  // generate new sql file with set search_path added at the begining
  newSqlFile =
      generateSQLFile(sqlFile, usingDefaultSchema, sqlFilePrefix, appendString);

  // outFile is located in the same folder with ansFile
  outputAbsPath = testRootPath + "/" + outputFile;
}

bool SQLUtility::__afterRunSqlfile(const std::string &initFile,
                                   bool sortoutfile, FilePath &fp,
                                   std::string &ansFileAbsPath,
                                   std::string &outputAbsPath,
                                   std::string &newSqlFile) {
  if (sortoutfile == false)
    return checkAnsFile(ansFileAbsPath, outputAbsPath, initFile, newSqlFile,
                        fp);
  else {
    hawq::test::Command comm;
    std::string command;
    fp = splitFilePath(outputAbsPath);
    if (fp.fileBaseName.empty()) {
      EXPECT_TRUE(false) << outputAbsPath << " is invalid";
      return false;
    }
    std::string sortoutFileAbsPath =
        fp.path + "/" + fp.fileBaseName + "_sort.out";
    command = "sort " + outputAbsPath + " > " + sortoutFileAbsPath;
    int result = comm.getCommandStatus(command);
    EXPECT_TRUE(result != -1);
    return checkAnsFile(ansFileAbsPath, sortoutFileAbsPath, initFile, "", fp);
  }
}

void SQLUtility::execAppendSQLFile(
    const string &sqlFile, const string &outputFile, const string &ansFile,
    const string &psqlOptions, const string &sqlFilePrefix,
    const string &appendString, const string &initFile, bool usingDefaultSchema,
    bool printTupleOnly, bool sortoutfile) {
  FilePath fp;
  std::string ansFileAbsPath, newSqlFile, outputAbsPath;
  __beforeRunSqlfile(sqlFile, outputFile, ansFile, sqlFilePrefix, appendString,
                     usingDefaultSchema, fp, ansFileAbsPath, newSqlFile,
                     outputAbsPath);
  conn->setOutputFile(outputAbsPath);
  EXPECT_EQ(0, conn->runSQLFile(newSqlFile, psqlOptions, printTupleOnly)
                   .getLastStatus());
  conn->resetOutput();
  __afterRunSqlfile(initFile, sortoutfile, fp, ansFileAbsPath, outputAbsPath,
                    newSqlFile);
}

void SQLUtility::runParallelSQLFile(const string sqlFile,
                                    const string outputFile, int processnum,
                                    const string ansFile,
                                    bool usingDefaultSchema) {
  // do precheck for sqlFile & ansFile
  if (hawq::test::startsWith(sqlFile, "/") ||
      hawq::test::startsWith(ansFile, "/"))
    ASSERT_TRUE(false) << "For sqlFile and ansFile, relative path to feature "
                          "test root dir is needed";
  // generate new sql file with set search_path added at the begining
  const string newSqlFile = generateSQLFile(sqlFile, usingDefaultSchema);
  pid_t childpid;
  std::vector<pid_t> childprocess;
  for (int i = 0; i < processnum; i++) {
    childpid = fork();
    if (childpid == -1) ASSERT_TRUE(false) << "Fork child process error";
    if (childpid == 0) {
      auto filename = hawq::test::split(outputFile, '.');
      string outFileAbsPath = testRootPath + "/" + filename[0] + "_process_" +
                              std::to_string(i) + ".out";
      conn->setOutputFile(outFileAbsPath);
      EXPECT_TRUE(conn->runSQLFile(newSqlFile).getLastStatus() == 0 ? true
                                                                    : false);
      exit(0);
    } else {
      childprocess.push_back(childpid);
    }
  }
  while (childprocess.size() > 0) {
    int length = childprocess.size();
    pid_t pid = waitpid(childprocess[length - 1], NULL, 0);
    if (pid == childprocess[length - 1]) childprocess.pop_back();
  }
}

void SQLUtility::runParallelSQLFile(const string sqlFile,
                                    const string outputFile, int processnum,
                                    std::vector<std::string> &options,
                                    const string ansFile,
                                    bool usingDefaultSchema) {
  // do precheck for sqlFile & ansFile
  if (hawq::test::startsWith(sqlFile, "/") ||
      hawq::test::startsWith(ansFile, "/"))
    ASSERT_TRUE(false) << "For sqlFile and ansFile, relative path to feature "
                          "test root dir is needed";
  // generate new sql file with set search_path added at the begining
  const string newSqlFile = generateSQLFile(sqlFile, usingDefaultSchema);
  pid_t childpid;
  std::vector<pid_t> childprocess;
  for (int i = 0; i < processnum; i++) {
    childpid = fork();
    if (childpid == -1) ASSERT_TRUE(false) << "Fork child process error";
    if (childpid == 0) {
      auto filename = hawq::test::split(outputFile, '.');
      string outFileAbsPath = testRootPath + "/" + filename[0] + "_process_" +
                              std::to_string(i) + ".out";
      conn->setOutputFile(outFileAbsPath);
      EXPECT_TRUE(conn->runSQLFile(newSqlFile, options[i]).getLastStatus() == 0
                      ? true
                      : false);
      exit(0);
    } else {
      childprocess.push_back(childpid);
    }
  }
  while (childprocess.size() > 0) {
    int length = childprocess.size();
    pid_t pid = waitpid(childprocess[length - 1], NULL, 0);
    if (pid == childprocess[length - 1]) childprocess.pop_back();
  }
}

void SQLUtility::__beforeExecSpecificSQLFile(
    const string &sqlFile, const string &outputFile, const string &ansFile,
    const string &sqlFilePrefix, bool usingDefaultSchema, FilePath &fp,
    string &ansFileAbsPath, string &newSqlFile, string &outputAbsPath) {
  // do precheck for sqlFile & ansFile
  if (hawq::test::startsWith(sqlFile, "/") ||
      hawq::test::startsWith(ansFile, "/"))
    ASSERT_TRUE(false) << "For sqlFile and ansFile, relative path to feature "
                          "test root dir is needed";
  ansFileAbsPath = testRootPath + "/" + ansFile;
  if (!std::ifstream(ansFileAbsPath))
    ASSERT_TRUE(false) << ansFileAbsPath << " doesn't exist";
  fp = splitFilePath(ansFileAbsPath);
  // double check to avoid empty fileBaseName
  if (fp.fileBaseName.empty())
    ASSERT_TRUE(false) << ansFileAbsPath << " is invalid";

  // generate new sql file with set search_path added at the begining
  newSqlFile = generateSQLFile(sqlFile, usingDefaultSchema, sqlFilePrefix);

  // outFile is located in the same folder with ansFile
  outputAbsPath = testRootPath + "/" + outputFile;
}

void SQLUtility::execSpecificSQLFile(
    const string &sqlFile, const string &outputFile, const string &ansFile,
    const string &psqlOptions, const string &sqlFilePrefix,
    const string &initFile, bool usingDefaultSchema, bool printTupleOnly) {
  FilePath fp;
  std::string ansFileAbsPath, newSqlFile, outputAbsPath;

  __beforeExecSpecificSQLFile(sqlFile, outputFile, ansFile, sqlFilePrefix,
                              usingDefaultSchema, fp, ansFileAbsPath,
                              newSqlFile, outputAbsPath);

  conn->setOutputFile(outputAbsPath);
  EXPECT_EQ(0, conn->runSQLFile(newSqlFile, psqlOptions, printTupleOnly)
                   .getLastStatus());
  conn->resetOutput();

  checkAnsFile(ansFileAbsPath, outputAbsPath, initFile, newSqlFile, fp);
}

const string SQLUtility::generateSQLFile(const string &sqlFile,
                                         bool usingDefaultSchema,
                                         const string &sqlFileSurfix,
                                         const string &appendString) {
  const string originSqlFile = testRootPath + "/" + sqlFile;
  const string newSqlFile = "/tmp/" + string(test_info->test_case_name()) +
                            "_" + test_info->name() + sqlFileSurfix + ".sql";
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
  if (sql_util_mode == MODE_MADLIB) {
    out << "SET SEARCH_PATH=" << schemaName << ",madlib;" << std::endl;
  } else if (!usingDefaultSchema) {
    out << "SET SEARCH_PATH=" + schemaName + ";" << std::endl;
  }
  if (sql_util_mode == MODE_DATABASE) {
    out << "\\c " << databaseName << std::endl;
  }
  out << "-- end_ignore" << std::endl;

  if (appendString.size() > 0) out << appendString << "\n";

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

std::string SQLUtility::getHawqDfsURL() {
  std::string url = this->getGUCValue("hawq_dfs_url");
  if (url[url.size() - 1] != '/') {
    url += "/";
  }
  return url;
}

bool SQLUtility::isDarwin() {
  std::string output = Command::getCommandOutput("uname | grep Darwin | wc -l");
  int num = atoi(output.c_str());
  return num != 0;
}

std::string SQLUtility::getHawqMagmaURL() {
  hawq::test::HawqConfig hc;
  std::vector<std::string> segs;
  hc.getSlaves(segs);

  if (segs.empty()) {
    return "";
  } else {
    // use magma service on first segment host specified in $GPHOME/etc/slaves
    return segs[0] + ":" + this->getGUCValue("hawq_magma_port_segment");
  }
}

std::string SQLUtility::getHawqDfsHost() {
  string url = this->getGUCValue("hawq_dfs_url");
  std::size_t pos = url.find("/");
  std::string result = url.substr(0, pos);
  return result;
}

std::string SQLUtility::getHdfsPath() {
  string url = this->getGUCValue("hawq_dfs_url");
  std::size_t found = url.find("/");
  std::string result = url.substr(found);
  if (result[result.size() - 1] != '/') {
    result += "/";
  }
  return result;
}

std::string SQLUtility::getHdfsNamenode() {
  string url = this->getGUCValue("hawq_dfs_url");
  std::size_t found = url.find("/");
  std::string result = url.substr(0, found + 1);
  return result;
}

const hawq::test::PSQL *SQLUtility::getPSQL() const { return conn.get(); }

string SQLUtility::getTestRootPath() {
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
  savedGUCValue += sql + ';';
}

std::string SQLUtility::getGUCValue(const std::string &guc) {
  string sql = "show " + guc;
  const hawq::test::PSQLQueryResult &result = executeQuery(sql);
  EXPECT_EQ(result.rowCount(), 1);
  if (result.rowCount() == 1) {
    std::vector<std::string> row = result.getRows()[0];
    return row[0];
  }
  return "";
}

std::string SQLUtility::getQueryResult(const std::string &query, bool check) {
  const hawq::test::PSQLQueryResult &result = executeQuery(query);
  std::string value;
  if (check) EXPECT_LE(result.rowCount(), 1);
  if (result.rowCount() == 1) {
    value = result.getRows()[0][0];
  } else {
    value = "";
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

FilePath SQLUtility::splitFilePath(const string &filePath) {
  FilePath fp;
  size_t found1 = filePath.find_last_of("/");
  size_t found2 = filePath.find_last_of(".");
  fp.path = filePath.substr(0, found1);
  fp.fileBaseName = filePath.substr(found1 + 1, found2 - found1 - 1);
  fp.fileSuffix = filePath.substr(found2 + 1, filePath.length() - found2 - 1);
  return fp;
}

void SQLUtility::runParallelSQLFile_sqllist(
    std::vector<std::string> &sqlfiles, std::vector<std::string> &ansfiles) {
  int len = sqlfiles.size();
  int anslen = ansfiles.size();
  int i;
  FilePath fp;
  std::string sqlFileAbsPath, ansFileAbsPath, outFileAbsPath;
  // do precheck for sqlFile & ansFile
  if (len != anslen)
    ASSERT_TRUE(false)
        << "The number of sqlfile is not equal to the number of ansfile";
  for (i = 0; i < len; i++) {
    if (hawq::test::startsWith(sqlfiles[i], "/") ||
        hawq::test::startsWith(ansfiles[i], "/"))
      ASSERT_TRUE(false) << "For sqlFile and ansFile, relative path to feature "
                            "test root dir is needed";
    ansFileAbsPath = testRootPath + "/" + ansfiles[i];
    if (!std::ifstream(ansFileAbsPath))
      ASSERT_TRUE(false) << ansFileAbsPath << " doesn't exist";
  }
  pid_t childpid;
  std::vector<pid_t> childprocess;
  for (i = 0; i < len; i++) {
    childpid = fork();
    if (childpid == -1) ASSERT_TRUE(false) << "Fork child process error";
    if (childpid == 0) {
      sqlFileAbsPath = testRootPath + "/" + sqlfiles[i];
      ansFileAbsPath = testRootPath + "/" + ansfiles[i];
      fp = splitFilePath(ansFileAbsPath);
      outFileAbsPath = fp.path + "/" + fp.fileBaseName + ".out";
      conn->setOutputFile(outFileAbsPath);
      EXPECT_TRUE(
          conn->runSQLFile(sqlFileAbsPath).getLastStatus() == 0 ? true : false);
      conn->resetOutput();
      checkAnsFile(ansFileAbsPath, outFileAbsPath, "", "", fp);
      exit(0);
    } else
      childprocess.push_back(childpid);
  }
  while (childprocess.size() > 0) {
    int length = childprocess.size();
    pid_t pid = waitpid(childprocess[length - 1], NULL, 0);
    if (pid == childprocess[length - 1]) childprocess.pop_back();
  }
}

}  // namespace test
}  // namespace hawq
