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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "policy_helper.h"
#include "gtest/gtest.h"
#include "lib/sql_util.h"

using namespace std;

class TestRangerPolicyHelper : public ::testing::Test {
public:
	TestRangerPolicyHelper() 
	{
		_rangerHost = RANGER_HOST;
	}
	string _rangerHost;
};

TEST_F(TestRangerPolicyHelper, BasicTest) {
	hawq::test::SQLUtility util;
	PolicyHelper helper(util.getTestRootPath(), _rangerHost);
	int ret = 0;
	string suffix = "PolicyHelper_";
	string user = suffix+"TestUser"; //Note: must create this user on Ranger Admin first
	string database = suffix+"Testdb";
	string table = suffix+"TestTable";
	string schema = "public";
	string protocol= "protocol";
	std::vector<std::string> accesses;
	
	ret = helper.AddSchemaPolicy(suffix+"Policy1", user, database, schema, {"usage-schema", "create-schema"});	
	EXPECT_EQ(0,ret);

	accesses.clear();
	accesses.push_back("select");
	ret = helper.AddTablePolicy(suffix+"Policy2", user, database, schema, table, accesses);	
	EXPECT_EQ(0,ret);
	
	accesses.clear();
	accesses.push_back("select");
	ret = helper.AddProtocolPolicy(suffix+"Policy3", user, protocol, accesses);	
	EXPECT_EQ(0,ret);

	ret = helper.ActivateAllPoliciesOnRanger();
	EXPECT_EQ(0,ret);

	ret = helper.DeletePolicyOnRanger(suffix+"Policy1");
	ret = helper.DeletePolicyOnRanger(suffix+"Policy2");
	ret = helper.DeletePolicyOnRanger(suffix+"Policy3");
	EXPECT_EQ(0,ret);
}
