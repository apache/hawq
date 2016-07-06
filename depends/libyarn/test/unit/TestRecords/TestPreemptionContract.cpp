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
#include <set>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "records/PreemptionContract.h"

using std::list;
using std::set;
using namespace libyarn;
using namespace testing;

class TestPreemptionContract: public ::testing::Test {
protected:
	PreemptionContract preemptionContract;
};

TEST_F(TestPreemptionContract, TestPreemptionContract)
{
	PreemptionContractProto proto;
	preemptionContract = PreemptionContract(proto);

	SUCCEED();
}

TEST_F(TestPreemptionContract, TestLocalGetProto)
{
	PreemptionContractProto proto;
	proto = preemptionContract.getProto();

	SUCCEED();
}

TEST_F(TestPreemptionContract, TestGetContainers)
{
	set<PreemptionContainer> containers;
	set<PreemptionContainer> retContainers;
	PreemptionContainer container;
	containers.insert(container);

	preemptionContract.setContainers(containers);
	retContainers = preemptionContract.getContainers();

	/* The operator '==' is not defined for class PreemptionContainer,
	   so we can't compare containers and retContainers here. */

	SUCCEED();
}

TEST_F(TestPreemptionContract, TestGetResourceRequest)
{
	list<PreemptionResourceRequest> requests;
	list<PreemptionResourceRequest> retRequests;
	PreemptionResourceRequest request;
	requests.push_back(request);

	preemptionContract.setResourceRequest(requests);
	retRequests = preemptionContract.getResourceRequest();

	/* The operator '==' is not defined for class PreemptionContainer,
	   so we can't compare containers and retContainers here. */

	SUCCEED();
}
