#ifndef SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
#define SRC_TEST_FEATURE_LIB_SQL_UTIL_H_

#include <string>

#include "gtest/gtest.h"
#include "psql.h"

#define HAWQ_DB (getenv("HAWQ_DB") ? getenv("HAWQ_DB") : "postgres")
#define HAWQ_HOST (getenv("HAWQ_HOST") ? getenv("HAWQ_HOST") : "localhost")
#define HAWQ_PORT (getenv("HAWQ_PORT") ? getenv("HAWQ_PORT") : "5432")
#define HAWQ_USER (getenv("HAWQ_USER") ? getenv("HAWQ_USER") : "gpadmin")
#define HAWQ_PASSWORD (getenv("HAWQ_PASSWORD") ? getenv("HAWQ_PASSWORD") : "")

class SQLUtility {
 public:
  SQLUtility() : conn(getConnection()) {
    const ::testing::TestInfo *const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    schemaName =
        std::string(test_info->test_case_name()) + "_" + test_info->name();
    execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
    execute("CREATE SCHEMA " + schemaName);
  }

  ~SQLUtility() { execute("DROP SCHEMA " + schemaName + " CASCADE"); }

  void execute(const std::string &sql) {
    conn->runSQLCommand("SET SEARCH_PATH=" + schemaName + ";" + sql);
    // should check the return code, depends on PSQL return code implementation
    // EXPECT_EQ(true, ret);
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
};

#endif  // SRC_TEST_FEATURE_LIB_SQL_UTIL_H_
