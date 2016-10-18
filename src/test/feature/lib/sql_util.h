#ifndef HAWQ_SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_SQL_UTIL_H_

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "psql.h"

#define HAWQ_DB (getenv("PGDATABASE") ? getenv("PGDATABASE") : "postgres")
#define HAWQ_HOST (getenv("PGHOST") ? getenv("PGHOST") : "localhost")
#define HAWQ_PORT (getenv("PGPORT") ? getenv("PGPORT") : "5432")
#define HAWQ_USER (getenv("PGUSER") ? getenv("PGUSER") : "")
#define HAWQ_PASSWORD (getenv("PGPASSWORD") ? getenv("PGPASSWORD") : "")
#define HAWQ_DEFAULT_SCHEMA ("public")

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
    MODE_MAX_NUM
};

class SQLUtility {
 public:
  SQLUtility(SQLUtilityMode mode = MODE_SCHEMA);
  ~SQLUtility();

  // Get the test database name
  // @return string of the test database name
  std::string getDbName();

  // Execute sql command
  // @param sql The given sql command
  // @param check true(default) if expected correctly executing, false otherwise
  // @return error or notice message if check is false, else return empty
  std::string execute(const std::string &sql, bool check = true);

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
  // @param initFile The given initFile (used by gpdiff.pl) which is relative path to test root dir
  // @return void
  void execSQLFile(const std::string &sqlFile, const std::string &ansFile, const std::string &initFile = "");

  // Execute sql file and check its return status
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @return true if the sql file is executed successfully, false otherwise
  bool execSQLFile(const std::string &sqlFile);

  // Get PSQL connection: do not suggest to use
  // @return PSQL raw pointer
  const hawq::test::PSQL *getPSQL() const;

  // Get test root dir abs path
  // @return path string
  std::string getTestRootPath() const;

  // Set GUC value
  void setGUCValue(const std::string &guc, const std::string &value);
  // Get GUC value
  std::string getGUCValue(const std::string &guc);

  // execute given query and return query result
  // @param query the given query
  // @return the query result
  std::string getQueryResult(const std::string &query);

  // execute expect error message
  // @param sql the given sql command
  // @param errmsg the expected sql error message
  // @return void
  void executeExpectErrorMsgStartWith(const std::string &sql, const std::string &errmsg);


 private:
  std::unique_ptr<hawq::test::PSQL> getConnection();
  const std::string generateSQLFile(const std::string &sqlFile);
  const hawq::test::PSQLQueryResult &executeQuery(const std::string &sql);
  FilePath splitFilePath(const std::string &filePath) const;
  void exec(const std::string &sql);

 private:
  std::string schemaName;
  std::string databaseName;
  SQLUtilityMode sql_util_mode;
  std::unique_ptr<hawq::test::PSQL> conn;
  std::string testRootPath;
  const ::testing::TestInfo *const test_info;
}; // class SQLUtility

} // namespace test
} // namespace hawq

#endif  // SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
