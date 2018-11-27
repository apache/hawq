#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestGpDistRandom: public ::testing::Test
{
	public:
		TestGpDistRandom() { }
		~TestGpDistRandom() {}
};

TEST_F(TestGpDistRandom, BasicTest)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("query/sql/goh_gp_dist_random.sql",
	                 "query/ans/goh_gp_dist_random.ans");
}
