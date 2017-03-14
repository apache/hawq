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

#ifndef TEST_HAWQ_RANGER_H
#define TEST_HAWQ_RANGER_H

#include "gtest/gtest.h"
#include "lib/sql_util.h"

class TestHawqRanger : public ::testing::Test {
public:
	TestHawqRanger();
	~TestHawqRanger() {
	}

	void clearEnv(hawq::test::SQLUtility* util, std::string case_name, int user_index);
	void runSQLFile(hawq::test::SQLUtility* util, std::string case_name,
			std::string ans_suffix, int sql_index = -1);

	void addPolicy(hawq::test::SQLUtility* util, std::string case_name, int policy_index);
	void addUser(hawq::test::SQLUtility* util, std::string case_name, int user_index = -1, bool full_policy = false,
			int writable_index = -1);

	std::string& getRangerHost();

private:
	std::string rangerHost = "";
	std::string initfile = "";

};

#endif
