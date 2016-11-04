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

#include "protocolrecords/GetQueueInfoRequest.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestGetQueueInfoRequest: public ::testing::Test {
protected:
	GetQueueInfoRequest getRequest;
};

TEST_F(TestGetQueueInfoRequest, TestGetQueueInfoRequest)
{
	GetQueueInfoRequestProto getRequestProto;
	getRequest = GetQueueInfoRequest(getRequestProto);

	SUCCEED();
}

TEST_F(TestGetQueueInfoRequest, TestGetQueueName)
{
	string queueName("queueName");
	string retQueueName;

	getRequest.setQueueName(queueName);
	retQueueName = getRequest.getQueueName();

	EXPECT_EQ(queueName, retQueueName);
}

TEST_F(TestGetQueueInfoRequest, TestGetIncludeApplications)
{
	bool includeApplications = true;
	bool retIncludeApplications;

	getRequest.setIncludeApplications(includeApplications);
	retIncludeApplications = getRequest.getIncludeApplications();

	EXPECT_EQ(includeApplications, retIncludeApplications);
}

TEST_F(TestGetQueueInfoRequest, TestGetIncludeChildQueues)
{
	bool includeChildQueues = true;
	bool retIncludeChildQueues;

	getRequest.setIncludeChildQueues(includeChildQueues);
	retIncludeChildQueues = getRequest.getIncludeChildQueues();

	EXPECT_EQ(includeChildQueues, retIncludeChildQueues);
}

TEST_F(TestGetQueueInfoRequest, TestGetRecursive)
{
	bool recursive = true;
	bool retRecursive;

	getRequest.setRecursive(recursive);
	retRecursive = getRequest.getRecursive();

	EXPECT_EQ(recursive, retRecursive);
}
