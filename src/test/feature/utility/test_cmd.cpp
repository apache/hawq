#include "gtest/gtest.h"

#include "lib/sql_util.h"

class TestCommand: public ::testing::Test {
 public:
  TestCommand() {}
  ~TestCommand() {}
};

TEST_F(TestCommand, TestCOPY) {
 hawq::test::SQLUtility util;
 util.execSQLFile("utility/sql/gpcopy.sql",
                  "utility/ans/gpcopy.ans");
}
