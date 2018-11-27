#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestTransaction: public ::testing::Test
{
	public:
		TestTransaction() { }
		~TestTransaction() {}
};

TEST_F(TestTransaction, BasicTest)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("transactions/sql/transactions.sql",
	                 "transactions/ans/transactions.ans");
}
