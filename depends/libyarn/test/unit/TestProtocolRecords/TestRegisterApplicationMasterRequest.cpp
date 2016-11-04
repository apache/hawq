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

#include "protocolrecords/RegisterApplicationMasterRequest.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestRegisterApplicationMasterRequest: public ::testing::Test {
protected:
	RegisterApplicationMasterRequest registerRequest;
};

TEST_F(TestRegisterApplicationMasterRequest, TestRegisterApplicationMasterRequest)
{
	RegisterApplicationMasterRequestProto registerRequestProto;
	registerRequest = RegisterApplicationMasterRequest(registerRequestProto);

	SUCCEED();
}

TEST_F(TestRegisterApplicationMasterRequest, TestGetHost)
{
	string host("host");
	string retHost;

	registerRequest.setHost(host);
	retHost = registerRequest.getHost();

	EXPECT_EQ(host, retHost);
}

TEST_F(TestRegisterApplicationMasterRequest, TestGetRpcPort)
{
	int32_t rpcPort = -1;
	int32_t retRpcPort;

	registerRequest.setRpcPort(rpcPort);
	retRpcPort = registerRequest.getRpcPort();

	EXPECT_EQ(rpcPort, retRpcPort);
}

TEST_F(TestRegisterApplicationMasterRequest, TestGetTrackingUrl)
{
	string trackingUrl("trackingUrl");
	string retTrackingUrl;

	registerRequest.setTrackingUrl(trackingUrl);
	retTrackingUrl = registerRequest.getTrackingUrl();

	EXPECT_EQ(trackingUrl, retTrackingUrl);
}
