#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestDatabase: public ::testing::Test
{
	public:
		TestDatabase() { }
		~TestDatabase() {}
};

TEST_F(TestDatabase, BasicTest)
{
	hawq::test::SQLUtility util(hawq::test::MODE_DATABASE);
	util.execSQLFile("ddl/sql/goh_database.sql",
	                 "ddl/ans/goh_database.ans",
                     "ddl/sql/init_file");
}
