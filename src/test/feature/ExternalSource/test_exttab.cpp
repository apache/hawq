/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

TEST_F(TestExternalTable, DISABLED_TestExternalTableAll) {
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
