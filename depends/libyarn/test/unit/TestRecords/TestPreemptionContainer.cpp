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

#include "records/PreemptionContainer.h"

using namespace libyarn;
using namespace testing;

class TestPreemptionContainer: public ::testing::Test {
protected:
	PreemptionContainer preemptionContainer;
};

TEST_F(TestPreemptionContainer, TestPreemptionContainer)
{
	PreemptionContainerProto proto;
	preemptionContainer = PreemptionContainer(proto);

	SUCCEED();
}

TEST_F(TestPreemptionContainer, TestLocalGetProto)
{
	PreemptionContainerProto proto;
	proto = preemptionContainer.getProto();

	SUCCEED();
}

TEST_F(TestPreemptionContainer, TestGetContainerId)
{
	ContainerId id;
	ContainerId retId;

	preemptionContainer.setId(id);
	retId = preemptionContainer.getId();

	/* The operator '==' is not defined for class ContainerId,
	   so we can't compare id and retId here. */

	SUCCEED();
}

TEST_F(TestPreemptionContainer, TestOperatorLessThan)
{
	int32_t id = -1;
	ApplicationId appId;
	appId.setId(id);
	ContainerId containerId;
	containerId.setApplicationId(appId);
	preemptionContainer.setId(containerId);

	int32_t id2 = 1;
	ApplicationId appId2;
	appId2.setId(id2);
	ContainerId containerId2;
	containerId2.setApplicationId(appId2);
	PreemptionContainer preemptionContainer2;
	preemptionContainer2.setId(containerId2);

	EXPECT_TRUE(preemptionContainer < preemptionContainer2);
	EXPECT_FALSE(preemptionContainer2 < preemptionContainer);
}
