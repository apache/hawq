#ifndef SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
#define SRC_TEST_FEATURE_LIB_SQL_UTIL_H_

#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "psql.h"

#define HAWQ_DB (getenv("PGDATABASE") ? getenv("PGDATABASE") : "postgres")
#define HAWQ_HOST (getenv("PGHOST") ? getenv("PGHOST") : "localhost")
#define HAWQ_PORT (getenv("PGPORT") ? getenv("PGPORT") : "5432")
#define HAWQ_USER (getenv("PGUSER") ? getenv("PGUSER") : "")
#define HAWQ_PASSWORD (getenv("PGPASSWORD") ? getenv("PGPASSWORD") : "")

struct FilePath {
  std::string path;
  std::string fileBaseName;
  std::string fileSuffix;
};

class SQLUtility {
 public:
  SQLUtility();
  ~SQLUtility();

  // Execute sql command
  // @param sql The given sql command
  // @return void
  void execute(const std::string &sql, bool check = true);

  // Execute query command and check the rowCount
  // @param sql The given query command
  // @expectNum The expected rowCount
  // @return void
  void query(const std::string &sql, int expectNum);

  // Execute query command and check query result
  // @param sql The given query command
  // @expectStr The given query result
  // @return void
  void query(const std::string &sql, const std::string &expectStr);

  // Execute sql file and diff with ans file
  // @param sqlFile The given sqlFile which is relative path to test root dir
  // @param ansFile The given ansFile which is relative path to test root dir
  // @return void
  void execSQLFile(const std::string &sqlFile, const std::string &ansFile);

  // Get PSQL connection
  // @return PSQL raw pointer
  const PSQL *getPSQL() const;

  // Get test root dir abs path
  // @return path string
  std::string getTestRootPath() const;

 private:
  std::unique_ptr<PSQL> getConnection();
  const std::string generateSQLFile(const std::string &sqlFile);
  const PSQLQueryResult &executeQuery(const std::string &sql);
  FilePath splitFilePath(const std::string &filePath) const;
  void exec(const std::string &sql);

 private:
  std::string schemaName;
  std::unique_ptr<PSQL> conn;
  std::string testRootPath;
  const ::testing::TestInfo *const test_info;
};

#endif  // SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
