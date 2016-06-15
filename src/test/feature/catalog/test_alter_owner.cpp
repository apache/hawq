#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <iostream>

#include "lib/command.h"
#include "lib/data_gen.h"
#include "lib/hawq_config.h"
#include "lib/sql_util.h"

#include "gtest/gtest.h"

class TestAlterOwner : public ::testing::Test {
 public:
  TestAlterOwner() {}
  ~TestAlterOwner() {}
};

TEST_F(TestAlterOwner, TestAlterOwnerAll) {
  hawq::test::SQLUtility util;
  util.execSQLFile("catalog/sql/alter-owner.sql",
		  	  	   "catalog/ans/alter-owner.ans");
}
