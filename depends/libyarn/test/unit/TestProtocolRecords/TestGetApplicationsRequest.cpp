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

#include <list>
#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "protocolrecords/GetApplicationsRequest.h"

using std::list;
using std::string;
using namespace libyarn;
using namespace testing;

class TestGetApplicationsRequest: public ::testing::Test {
protected:
	GetApplicationsRequest getRequest;
};

TEST_F(TestGetApplicationsRequest, TestGetApplicationRequest)
{
	GetApplicationsRequestProto getRequestProto;
	getRequest = GetApplicationsRequestProto(getRequestProto);

	SUCCEED();
}

TEST_F(TestGetApplicationsRequest, TestGetApplicationTypes)
{
	list<string> applicationTypes;
	list<string> retApplicationTypes;
	string type("type");
	applicationTypes.push_back(type);

	getRequest.setApplicationTypes(applicationTypes);
	retApplicationTypes = getRequest.getApplicationTypes();

	EXPECT_TRUE(applicationTypes == retApplicationTypes);
}

TEST_F(TestGetApplicationsRequest, TestGetApplicationStatus)
{
	list<YarnApplicationState> applicationStates;
	list<YarnApplicationState> retApplicationStates;
	YarnApplicationState state;
	applicationStates.push_back(state);

	getRequest.setApplicationStates(applicationStates);
	retApplicationStates = getRequest.getApplicationStates();

	EXPECT_TRUE(applicationStates == retApplicationStates);
}
