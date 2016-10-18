#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestInformationSchema: public ::testing::Test
{
	public:
		TestInformationSchema() { }
		~TestInformationSchema() {}
};

TEST_F(TestInformationSchema, BasicTest)
{
	hawq::test::SQLUtility util(hawq::test::MODE_DATABASE);
	util.execSQLFile("query/sql/information_schema.sql",
	                 "query/ans/information_schema.ans");
}
