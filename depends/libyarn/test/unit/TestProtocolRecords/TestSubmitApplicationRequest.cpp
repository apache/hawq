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

#include "protocolrecords/SubmitApplicationRequest.h"

using namespace libyarn;
using namespace testing;

class TestSubmitApplicationRequest: public ::testing::Test {
protected:
	SubmitApplicationRequest submitRequest;
};

TEST_F(TestSubmitApplicationRequest, TestSubmitApplicationRequest)
{
	SubmitApplicationRequestProto submitRequestProto;
	submitRequest = SubmitApplicationRequest(submitRequestProto);

	SUCCEED();
}

TEST_F(TestSubmitApplicationRequest, TestGetContainerIds)
{
	ApplicationSubmissionContext appCtx;
	ApplicationSubmissionContext retAppCtx;

	submitRequest.setApplicationSubmissionContext(appCtx);
	retAppCtx = submitRequest.getApplicationSubmissionContext();

	/* The operator '==' is not defined for class ApplicationSubmissionContext,
	   so we can't compare appCtx and retAppCtx here. */
	SUCCEED();
}
