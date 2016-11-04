#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestTemp: public ::testing::Test
{
	public:
		TestTemp() { }
		~TestTemp() {}
};

TEST_F(TestTemp, BasicTest)
{
	hawq::test::SQLUtility util(hawq::test::MODE_DATABASE);
	util.execSQLFile("query/sql/temp.sql",
	                 "query/ans/temp.ans",
                     "query/sql/init_file");
}
