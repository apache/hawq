#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestType: public ::testing::Test
{
	public:
		TestType() {};
		~TestType() {};
};

#define TEST_F_FILE(TestName, basePath, testcase)	\
TEST_F(TestName, testcase)							\
{													\
	hawq::test::SQLUtility util;					\
	string SqlFile(basePath);						\
	string AnsFile(basePath);						\
	SqlFile += "/sql/" #testcase ".sql";			\
	AnsFile += "/ans/" #testcase ".ans";			\
	util.execSQLFile(SqlFile, AnsFile);				\
}

#define TEST_F_FILE_TYPE(testcase) TEST_F_FILE(TestType, "catalog", testcase)

TEST_F_FILE_TYPE(boolean)

TEST_F_FILE_TYPE(char)

TEST_F_FILE_TYPE(date)

TEST_F_FILE_TYPE(float4)

TEST_F_FILE_TYPE(float8)

TEST_F_FILE_TYPE(int2)

TEST_F_FILE_TYPE(int4)

TEST_F_FILE_TYPE(int8)

TEST_F_FILE_TYPE(money)

TEST_F_FILE_TYPE(name)

TEST_F_FILE_TYPE(oid)

TEST_F_FILE_TYPE(text)

TEST_F_FILE_TYPE(time)

TEST_F(TestType, type_sanity)
{
	hawq::test::SQLUtility util(hawq::test::MODE_DATABASE);
	util.execSQLFile("catalog/sql/type_sanity.sql",
	                 "catalog/ans/type_sanity.ans");
}

TEST_F_FILE_TYPE(varchar)
