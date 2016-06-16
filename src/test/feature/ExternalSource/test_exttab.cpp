#include "gtest/gtest.h"

#include "lib/sql_util.h"
#include "lib/file_replace.h"

using hawq::test::SQLUtility;
using hawq::test::FileReplace;

class TestExternalTable : public ::testing::Test {
 public:
  TestExternalTable() {}
  ~TestExternalTable() {}
};

TEST_F(TestExternalTable, TestExternalTableAll) {
  SQLUtility util;
  auto test_root = util.getTestRootPath();
  auto replace_lambda = [&] () {
    FileReplace frep;
    std::unordered_map<std::string, std::string> D;
    D["@hostname@"] = std::string("localhost");
    D["@abs_srcdir@"] = test_root + "/ExternalSource";
    D["@gpwhich_gpfdist@"] = std::string(std::string(getenv("GPHOME")) + "/bin/gpfdist");
    frep.replace(test_root + "/ExternalSource/sql/exttab1.sql.source",
                 test_root + "/ExternalSource/sql/exttab1.sql",
                 D);
    frep.replace(test_root + "/ExternalSource/ans/exttab1.ans.source",
                 test_root + "/ExternalSource/ans/exttab1.ans",
                 D);
  };
  
  replace_lambda();
  util.execSQLFile("ExternalSource/sql/exttab1.sql",
                   "ExternalSource/ans/exttab1.ans");
}
