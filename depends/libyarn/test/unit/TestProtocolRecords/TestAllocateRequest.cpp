/*
 * Licensed to the Apache SoftwallocateRequeste Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regallocateRequestding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * softwallocateRequeste distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <list>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "protocolrecords/AllocateRequest.h"

using std::list;
using namespace libyarn;
using namespace testing;

class TestAllocateRequest: public ::testing::Test {
protected:
	AllocateRequest allocateRequest;
};

TEST_F(TestAllocateRequest, TestAddAsk)
{
	ResourceRequest ask;

	allocateRequest.addAsk(ask);

	SUCCEED();
}

TEST_F(TestAllocateRequest, TestGetAsks)
{
	ResourceRequest ask;
	list<ResourceRequest> asks;

	allocateRequest.addAsk(ask);
	asks = allocateRequest.getAsks();

	SUCCEED();
}

TEST_F(TestAllocateRequest, TestAddRelease)
{
	ContainerId release;

	allocateRequest.addRelease(release);

	SUCCEED();
}

TEST_F(TestAllocateRequest, TestGetRelease)
{
	ContainerId release;
	list<ContainerId> releases;

	allocateRequest.addRelease(release);
	releases = allocateRequest.getReleases();

	SUCCEED();
}

TEST_F(TestAllocateRequest, TestGetBlackListRequest)
{
	ResourceBlacklistRequest blacklistRequest;

	blacklistRequest = allocateRequest.getBlacklistRequest();

	SUCCEED();
}

TEST_F(TestAllocateRequest, TestGetResponseId)
{
	int32_t responseId = -1;
	int32_t retResponseId;

	allocateRequest.setResponseId(responseId);
	retResponseId = allocateRequest.getResponseId();
	EXPECT_EQ(responseId, retResponseId);

}

TEST_F(TestAllocateRequest, TestGetProgress)
{
	float progress = 0.0;
	float retProgress;

	allocateRequest.setProgress(progress);
	retProgress = allocateRequest.getProgress();
	EXPECT_EQ(progress, retProgress);
}
