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

#include "protocolrecords/GetClusterNodesRequest.h"

using std::list;
using namespace libyarn;
using namespace testing;

class TestGetClusterNodesRequest: public ::testing::Test {
protected:
	GetClusterNodesRequest getRequest;
};

TEST_F(TestGetClusterNodesRequest, TestGetClusterMetricsRequest)
{
	GetClusterNodesRequestProto getRequestProto;
	getRequest = GetClusterNodesRequest(getRequestProto);

	SUCCEED();
}

TEST_F(TestGetClusterNodesRequest, TestGetNodeStates)
{
	list<NodeState> states;
	list<NodeState> retStates;
	NodeState state;
	states.push_back(state);

	getRequest.setNodeStates(states);
	retStates = getRequest.getNodeStates();

	EXPECT_TRUE(states == retStates);
}
