#include "gtest/gtest.h"

#include "lib/sql_util.h"

using std::string;

class TestRowTypes : public ::testing::Test
{
	public:
		TestRowTypes() { }
		~TestRowTypes() {}
};

TEST_F(TestRowTypes, BasicTest)
{
	hawq::test::SQLUtility util;

    util.execute("drop table if exists tenk1");
    util.execute("create table tenk1 ("
                "    unique1     int4,"
                "    unique2     int4,"
                "    two         int4,"
                "    four        int4,"
                "    ten         int4,"
                "    twenty      int4,"
                "    hundred     int4,"
                "    thousand    int4,"
                "    twothousand int4,"
                "    fivethous   int4,"
                "    tenthous    int4,"
                "    odd         int4,"
                "    even        int4,"
                "    stringu1    name,"
                "    stringu2    name,"
                "    string4     name) with oids");
	
    std::string pwd = util.getTestRootPath();
    std::string cmd = "COPY tenk1 FROM '" + pwd + "/query/data/tenk.data'";
    std::cout << cmd << std::endl;
    util.execute(cmd);
    
    util.execSQLFile("query/sql/rowtypes.sql",
	                 "query/ans/rowtypes.ans");
}
