#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <iostream>

#include "lib/command.h"
#include "lib/common.h"
#include "lib/data-gen.h"
#include "lib/hawq-config.h"
#include "lib/sql-util.h"

#include "gtest/gtest.h"

class TestQueryPrepare : public ::testing::Test {
 public:
  TestQueryPrepare() {}
  ~TestQueryPrepare() {}
};


TEST_F(TestQueryPrepare, TestPrepareUniqueness) {
  SQLUtility util;
  util.execSQLFile("query/sql/prepare-uniqueness.sql",
		  	  	   "query/ans/prepare-uniqueness.ans");
}

TEST_F(TestQueryPrepare, TestPrepareParameters) {
  SQLUtility util;
  // prepare
  util.execute("drop table if exists test1");
  util.execute("drop table if exists test2");
  util.execute("create table test1 ("
		  	   "	unique1		int4,"
		  	   "	unique2		int4,"
		  	   "	two			int4,"
		  	   "	four		int4,"
		  	   "	ten			int4,"
		  	   "	twenty		int4,"
		  	   "	hundred		int4,"
		  	   "	thousand	int4,"
		  	   "	twothousand	int4,"
		  	   "	fivethous	int4,"
		  	   "	tenthous	int4,"
		  	   "	odd			int4,"
		  	   "	even		int4,"
		  	   "	stringu1	name,"
		  	   "	stringu2	name,"
		  	   "	string4		name) with oids");
  util.execute("create table test2 ("
		  	   "	name		text,"
		  	   "	thepath		path)");

  std::string pwd = util.getTestRootPath();
  std::string cmd = "COPY test1 FROM '" + pwd + "/query/data/tenk.data'";
  std::cout << cmd << std::endl;
  util.execute(cmd);
  cmd = "COPY test2 FROM '" + pwd + "/query/data/streets.data'";
  std::cout << cmd << std::endl;
  util.execute(cmd);

  // do test
  util.execSQLFile("query/sql/prepare-parameters.sql",
		  	  	   "query/ans/prepare-parameters.ans");

  // cleanup
  util.execute("drop table test1");
  util.execute("drop table test2");
}
