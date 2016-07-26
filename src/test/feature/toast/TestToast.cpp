#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestToast: public ::testing::Test
{
	public:
		TestToast() { }
		~TestToast() {}
};

TEST_F(TestToast, BasicTest)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("toast/sql/goh_toast.sql",
	                 "toast/ans/goh_toast.ans");
}
