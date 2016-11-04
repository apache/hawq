#include <string>
#include <iostream>
#include "gtest/gtest.h"

#include "lib/sql_util.h"
#include "lib/file_replace.h"

using hawq::test::SQLUtility;
using hawq::test::FileReplace;

class TestExternalOid : public ::testing::Test {
 public:
  TestExternalOid() {}
  ~TestExternalOid() {}
};

TEST_F(TestExternalOid, TestExternalOidAll) {
  SQLUtility util;
  FileReplace frep;
  auto test_root = util.getTestRootPath();
  std::cout << test_root << std::endl;

  std::unordered_map<std::string, std::string> D;
  D["@SHARE_LIBRARY_PATH@"] = test_root + "/ExternalSource/lib/function.so";
  D["@abs_datadir@"] = test_root + "/ExternalSource/data";
  frep.replace(test_root + "/ExternalSource/sql/external_oid.sql.source",
               test_root + "/ExternalSource/sql/external_oid.sql",
               D);
  frep.replace(test_root + "/ExternalSource/ans/external_oid.ans.source",
               test_root + "/ExternalSource/ans/external_oid.ans",
               D);
 
  util.execSQLFile("ExternalSource/sql/external_oid.sql",
                   "ExternalSource/ans/external_oid.ans");
}
