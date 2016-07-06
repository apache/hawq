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

#include "protocolrecords/StartContainersResponse.h"

using std::list;
using namespace libyarn;
using namespace testing;

class TestStartContainersResponse: public ::testing::Test {
protected:
	StartContainersResponse startResponse;
};

TEST_F(TestStartContainersResponse, TestStartContainersGetProto)
{
	StartContainersResponseProto startResponseProto;
	startResponseProto = startResponse.getProto();

	SUCCEED();
}

TEST_F(TestStartContainersResponse, TestGetSucceededRequests)
{
	list<ContainerId> requests;
	list<ContainerId> retRequests;
	ContainerId containerId;
	requests.push_back(containerId);

	startResponse.setSucceededRequests(requests);
	retRequests = startResponse.getSucceededRequests();

	/* The operator '==' is not defined for class ContainerId,
	   so we can't compare requests and retRequests here. */

	SUCCEED();
}

TEST_F(TestStartContainersResponse, TestGetFailedRequests)
{
	list<ContainerExceptionMap> requests;
	list<ContainerExceptionMap> retRequests;
	ContainerExceptionMap map;
	requests.push_back(map);

	startResponse.setFailedRequests(requests);
	retRequests = startResponse.getFailedRequests();

	/* The operator '==' is not defined for class ContainerExceptionMap,
	   so we can't compare requests and retRequests here. */

	SUCCEED();
}
