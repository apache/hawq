#include "gtest/gtest.h"

#include "lib/sql_util.h"


class TestPreparedStatement: public ::testing::Test
{
	public:
		TestPreparedStatement() {}
		~TestPreparedStatement() {}
};

// HAWQ-800: https://issues.apache.org/jira/browse/HAWQ-800
// HAWQ-835: https://issues.apache.org/jira/browse/HAWQ-835
TEST_F(TestPreparedStatement, TestPreparedStatementPrepare)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("PreparedStatement/sql/proba.sql",
	                 "PreparedStatement/ans/proba.ans");
}

// HAWQ-800: https://issues.apache.org/jira/browse/HAWQ-800
// HAWQ-835: https://issues.apache.org/jira/browse/HAWQ-835
TEST_F(TestPreparedStatement, TestPreparedStatementExecute)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("PreparedStatement/sql/proba_execute.sql",
	                 "PreparedStatement/ans/proba_execute.ans");
}

// HAWQ-800: https://issues.apache.org/jira/browse/HAWQ-800
// HAWQ-835: https://issues.apache.org/jira/browse/HAWQ-835
TEST_F(TestPreparedStatement, TestPreparedStatementInsert)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("PreparedStatement/sql/insert.sql",
	                 "PreparedStatement/ans/insert.ans");
}
