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

#include "protocolrecords/StartContainerRequest.h"

using namespace libyarn;
using namespace testing;

class TestStartContainerRequest: public ::testing::Test {
protected:
	StartContainerRequest startRequest;
};

TEST_F(TestStartContainerRequest, TestStartContainerRequest)
{
	StartContainerRequestProto startRequestProto;
	startRequest = StartContainerRequest(startRequestProto);

	SUCCEED();
}

TEST_F(TestStartContainerRequest, TestGetContainerLaunchCtx)
{
	ContainerLaunchContext ctx;
	ContainerLaunchContext retCtx;

	startRequest.setContainerLaunchCtx(ctx);
	retCtx = startRequest.getContainerLaunchCtx();

	/* The operator '==' is not defined for class ContainerLaunchContext,
	   so we can't compare ctx and retCtx here. */
	SUCCEED();
}

TEST_F(TestStartContainerRequest, TestGetContainerToken)
{
	Token token;
	Token retToken;

	startRequest.setContainerToken(token);
	retToken = startRequest.getContainerToken();

	/* The operator '==' is not defined for class Token,
	   so we can't compare token and retToken here. */
	SUCCEED();
}
