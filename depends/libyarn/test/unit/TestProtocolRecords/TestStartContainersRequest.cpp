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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "protocolrecords/StartContainersRequest.h"

using std::list;
using namespace libyarn;
using namespace testing;

class TestStartContainersRequest: public ::testing::Test {
protected:
	StartContainersRequest startRequest;
};

TEST_F(TestStartContainersRequest, TestStartContainersRequest)
{
	StartContainersRequestProto startRequestProto;
	startRequest = StartContainersRequest(startRequestProto);

	SUCCEED();
}

TEST_F(TestStartContainersRequest, TestGetStartContainerRequests)
{
	list<StartContainerRequest> requests;
	list<StartContainerRequest> retRequests;
	StartContainerRequest request;
	requests.push_back(request);

	startRequest.setStartContainerRequests(requests);
	retRequests = startRequest.getStartContainerRequests();

	/* The operator '==' is not defined for class StartContainerRequest,
	   so we can't compare requests and retRequests here. */
	SUCCEED();
}
