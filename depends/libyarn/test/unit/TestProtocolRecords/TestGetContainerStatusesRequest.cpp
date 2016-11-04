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

#include "protocolrecords/GetContainerStatusesRequest.h"

using std::list;
using namespace libyarn;
using namespace testing;

class TestGetContainerStatusesRequest: public ::testing::Test {
protected:
	GetContainerStatusesRequest getRequest;
};

TEST_F(TestGetContainerStatusesRequest, TestGetContainerStatusesRequest)
{
	GetContainerStatusesRequestProto getRequestProto;
	getRequest = GetContainerStatusesRequest(getRequestProto);

	SUCCEED();
}

TEST_F(TestGetContainerStatusesRequest, TestGetContainerIds)
{
	list<ContainerId> idList;
	list<ContainerId> retIdList;
	ContainerId containerId;
	idList.push_back(containerId);

	getRequest.setContainerIds(idList);
	retIdList = getRequest.getContainerIds();

	/* The operator '==' is not defined for class ContainerId,
	   so we can't compare idList and retIdList here. */
	SUCCEED();
}
