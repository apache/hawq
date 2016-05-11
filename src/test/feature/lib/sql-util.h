#ifndef SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
#define SRC_TEST_FEATURE_LIB_SQL_UTIL_H_

#include <string>
#include "gtest/gtest.h"
#include "psql.h"

#define HAWQ_DB (getenv("PGDATABASE") ? getenv("PGDATABASE") : "postgres")
#define HAWQ_HOST (getenv("PGHOST") ? getenv("PGHOST") : "localhost")
#define HAWQ_PORT (getenv("PGPORT") ? getenv("PGPORT") : "5432")
#define HAWQ_USER (getenv("PGUSER") ? getenv("PGUSER") : "taoz")
#define HAWQ_PASSWORD (getenv("PGPASSWORD") ? getenv("PGPASSWORD") : "")

class SQLUtility {
 public:
  SQLUtility()
      : conn(getConnection()),
        test_info(::testing::UnitTest::GetInstance()->current_test_info()) {
    schemaName =
        std::string(test_info->test_case_name()) + "_" + test_info->name();
    conn->runSQLCommand("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
    conn->runSQLCommand("CREATE SCHEMA " + schemaName);
  }

  ~SQLUtility() {
    if (!test_info->result()->Failed())
      conn->runSQLCommand("DROP SCHEMA " + schemaName + " CASCADE");
  }

  void execute(const std::string &sql) {
    EXPECT_EQ(conn->runSQLCommand("SET SEARCH_PATH= " + schemaName + ";" + sql)
                  .getLastStatus(),
              0);
  }

  void query(const std::string &sql, int resultNum) {
    PSQLQueryResult result =
        conn->getQueryResult("SET SEARCH_PATH= " + schemaName + ";" + sql);
    EXPECT_EQ(result.rowCount(), resultNum);
  }

 private:
  std::unique_ptr<PSQL> getConnection() {
    std::unique_ptr<PSQL> psql(
        new PSQL(HAWQ_DB, HAWQ_HOST, HAWQ_PORT, HAWQ_USER, HAWQ_PASSWORD));
    return std::move(psql);
  }

 private:
  std::string schemaName;
  std::unique_ptr<PSQL> conn;
  const ::testing::TestInfo *const test_info;
};

#endif  // SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
