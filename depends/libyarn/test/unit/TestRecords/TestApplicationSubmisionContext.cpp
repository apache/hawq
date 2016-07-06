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

#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "records/ApplicationSubmissionContext.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestApplicationSubmissionContext: public ::testing::Test {
protected:
	ApplicationSubmissionContext applicationContext;
};

TEST_F(TestApplicationSubmissionContext, TestGetApplicationId)
{
	ApplicationId id;
	ApplicationId retId;

	applicationContext.setApplicationId(id);
	retId = applicationContext.getApplicationId();

	/* The operator '==' is not defined for class ApplicationId,
	   so we can't compare id and retId here. */

	SUCCEED();
}

TEST_F(TestApplicationSubmissionContext, TestGetApplicationName)
{
	string name("name");
	string retName;

	applicationContext.setApplicationName(name);
	retName = applicationContext.getApplicationName();

	EXPECT_EQ(name, retName);
}

TEST_F(TestApplicationSubmissionContext, TestGetQueue)
{
	string queue("queue");
	string retQueue;

	applicationContext.setQueue(queue);
	retQueue = applicationContext.getQueue();

	EXPECT_EQ(queue, retQueue);
}

TEST_F(TestApplicationSubmissionContext, TestGetPriority)
{
	Priority priority;
	Priority retPriority;

	applicationContext.setPriority(priority);
	retPriority = applicationContext.getPriority();

	/* The operator '==' is not defined for class Priority,
	   so we can't compare priority and retPriority here. */

	SUCCEED();
}

TEST_F(TestApplicationSubmissionContext, TestGetAMContainerSpec)
{
	ContainerLaunchContext context;
	ContainerLaunchContext retContext;

	applicationContext.setAMContainerSpec(context);
	retContext = applicationContext.getAMContainerSpec();

	/* The operator '==' is not defined for class ContainerLaunchContext,
	   so we can't compare context and retContext here. */

	SUCCEED();
}

TEST_F(TestApplicationSubmissionContext, TestGetCancelTokensWhenComplete)
{
	bool flag = true;
	bool retFlag;

	applicationContext.setCancelTokensWhenComplete(flag);
	retFlag = applicationContext.getCancelTokensWhenComplete();

	EXPECT_EQ(flag, retFlag);
}

TEST_F(TestApplicationSubmissionContext, TestGetUnmanagedAM)
{
	bool flag = true;
	bool retFlag;

	applicationContext.setUnmanagedAM(flag);
	retFlag = applicationContext.getUnmanagedAM();

	EXPECT_EQ(flag, retFlag);
}

TEST_F(TestApplicationSubmissionContext, TestGetMaxAppAttempts)
{
	int32_t max = -1;
	int32_t retMax;

	applicationContext.setMaxAppAttempts(max);
	retMax = applicationContext.getMaxAppAttempts();

	EXPECT_EQ(max, retMax);
}

TEST_F(TestApplicationSubmissionContext, TestGetResource)
{
	Resource resource;
	Resource retResource;

	applicationContext.setResource(resource);
	retResource = applicationContext.getResource();

	/* The operator '==' is not defined for class Resource,
	   so we can't compare resource and retResource here. */

	SUCCEED();
}

TEST_F(TestApplicationSubmissionContext, TestGetApplicationType)
{
	string type("type");
	string retType;

	applicationContext.setApplicationType(type);
	retType = applicationContext.getApplicationType();

	EXPECT_EQ(type, retType);
}
