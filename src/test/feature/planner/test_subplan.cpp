#include "gtest/gtest.h"

#include "lib/sql_util.h"

class TestSubplan : public ::testing::Test {
 public:
  TestSubplan() {}
  ~TestSubplan() {}
};

TEST_F(TestSubplan, TestSubplanAll) {
 hawq::test::SQLUtility util;
 util.execSQLFile("planner/sql/subplan.sql",
                  "planner/ans/subplan.ans");
}
