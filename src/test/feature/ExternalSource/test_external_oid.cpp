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
