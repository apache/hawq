#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <iostream>
#include <string>

#include "lib/sql_util.h"

#include "gtest/gtest.h"

class TestRegex : public ::testing::Test {
 public:
  TestRegex() {}
  ~TestRegex() {}
};


TEST_F(TestRegex, TestRegexBasic) {
  hawq::test::SQLUtility util;
  util.execSQLFile("regex/sql/regex_basic.sql",
		  	  	   "regex/ans/regex_basic.ans");
}

