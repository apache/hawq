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

using std::string;
using hawq::test::FileReplace;

class TestGuc: public ::testing::Test
{
	public:
		TestGuc() {};
		~TestGuc() {};
};

// Mainly test per-db guc via "alter database" which installcheck-good does not seem to cover.
// Need to include other guc test cases (at least installcheck-good: guc.sql/guc.ans)
TEST_F(TestGuc, per_db_guc_with_space)
{
	hawq::test::SQLUtility util(hawq::test::MODE_DATABASE);
	string cmd;

	cmd  = "alter database " + util.getDbName() + " set datestyle to 'sql, mdy'";
	util.execute(cmd);

	util.execSQLFile("catalog/sql/guc.sql", "catalog/ans/guc.ans");
}

