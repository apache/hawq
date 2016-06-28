#include "gtest/gtest.h"

#include "lib/sql_util.h"
#include "lib/file_replace.h"


class TestUDF: public ::testing::Test
{
	public:
		TestUDF() {}
		~TestUDF() {}
};

TEST_F(TestUDF, TestUDFBasics)
{
	hawq::test::SQLUtility util;
	if (util.getGUCValue("optimizer") == "on")
	{
		util.execSQLFile("UDF/sql/function_basics.sql",
		                 "UDF/ans/function_basics.ans.orca");
	}
	else
	{
		util.execSQLFile("UDF/sql/function_basics.sql",
		                 "UDF/ans/function_basics.ans.planner");
	}
}

TEST_F(TestUDF, TestUDFCreation)
{
	// preprocess source files to get sql/ans files
	hawq::test::SQLUtility util;
	std::string d_feature_test_root(util.getTestRootPath());
	std::string f_sql_tpl(d_feature_test_root + "/UDF/sql/function_creation.sql.source");
	std::string f_ans_tpl(d_feature_test_root + "/UDF/ans/function_creation.ans.source");
	std::string f_sql(d_feature_test_root + "/UDF/sql/function_creation.sql");
	std::string f_ans(d_feature_test_root + "/UDF/ans/function_creation.ans");

	hawq::test::FileReplace frep;
	std::unordered_map<std::string, std::string> strs_src_dst;
	strs_src_dst["@SHARE_LIBRARY_PATH@"] = d_feature_test_root + "/UDF/lib/function.so";

	frep.replace(f_sql_tpl, f_sql, strs_src_dst);
	frep.replace(f_ans_tpl, f_ans, strs_src_dst);

	// run sql file to get ans file and then diff it with out file
	util.execSQLFile("UDF/sql/function_creation.sql",
	                 "UDF/ans/function_creation.ans");
}

TEST_F(TestUDF, TestUDFSetReturning)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("UDF/sql/function_set_returning.sql",
	                 "UDF/ans/function_set_returning.ans");
}

TEST_F(TestUDF, TestUDFExtension)
{
	hawq::test::SQLUtility util;
	util.execSQLFile("UDF/sql/function_extension.sql",
	                 "UDF/ans/function_extension.ans");
}
