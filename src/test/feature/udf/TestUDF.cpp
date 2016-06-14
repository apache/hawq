#include "gtest/gtest.h"

#include "lib/sql_util.h"


class TestUDF: public ::testing::Test
{
	public:
		TestUDF() {}
		~TestUDF() {}
};

TEST_F(TestUDF, TestUDFBasics)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("udf/sql/function_basics.sql",
	                 "udf/ans/function_basics.ans");
}

TEST_F(TestUDF, TestUDFSetReturning)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("udf/sql/function_set_returning.sql",
	                 "udf/ans/function_set_returning.ans");
}

TEST_F(TestUDF, TestUDFExtension)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("udf/sql/function_extension.sql",
	                 "udf/ans/function_extension.ans");
}
