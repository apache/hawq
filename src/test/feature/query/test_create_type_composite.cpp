#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestCreateTypeComposite: public ::testing::Test
{
	public:
		TestCreateTypeComposite() { }
		~TestCreateTypeComposite() {}
};

TEST_F(TestCreateTypeComposite, BasicTest)
{
	hawq::test::SQLUtility util(hawq::test::MODE_DATABASE);
	util.execSQLFile("query/sql/goh_create_type_composite.sql",
	                 "query/ans/goh_create_type_composite.ans",
                     "query/sql/init_file");
}
