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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "records/PreemptionMessage.h"

using namespace libyarn;
using namespace testing;

class TestPreemptionMessage: public ::testing::Test {
protected:
	PreemptionMessage preemptionMessage;
};

TEST_F(TestPreemptionMessage, TestPreemptionMessage)
{
	PreemptionMessageProto proto;
	preemptionMessage = PreemptionMessage(proto);

	SUCCEED();
}

TEST_F(TestPreemptionMessage, TestLocalGetProto)
{
	PreemptionMessageProto proto;
	proto = preemptionMessage.getProto();

	SUCCEED();
}

TEST_F(TestPreemptionMessage, TestGetStrictContract)
{
	StrictPreemptionContract contract;
	StrictPreemptionContract retContract;

	preemptionMessage.setStrictContract(contract);
	retContract = preemptionMessage.getStrictContract();

	/* The operator '==' is not defined for class StricPreemptionContract,
	   so we can't compare contract and retContract here. */

	SUCCEED();
}

TEST_F(TestPreemptionMessage, TestGetContract)
{
	PreemptionContract contract;
	PreemptionContract retContract;

	preemptionMessage.setContract(contract);
	retContract = preemptionMessage.getContract();

	/* The operator '==' is not defined for class PreemptionContract,
	   so we can't compare contract and retContract here. */

	SUCCEED();
}
