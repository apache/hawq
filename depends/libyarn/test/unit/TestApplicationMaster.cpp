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

#include "libyarnclient/ApplicationMaster.h"
#include "MockApplicationMasterProtocol.h"

using namespace std;
using namespace libyarn;
using namespace testing;
using namespace Mock;

class TestApplicationMaster: public ::testing::Test {
public:
	TestApplicationMaster(){
		string schedHost("localhost");
		string schedPort("8032");
		string tokenService = "";
		Yarn::Config config;
		Yarn::Internal::SessionConfig sessionConfig(config);
		Yarn::Internal::UserInfo user = Yarn::Internal::UserInfo::LocalUser();
		Yarn::Internal::RpcAuth rpcAuth(user, Yarn::Internal::AuthMethod::SIMPLE);
		MockApplicationMasterProtocol *protocol = new MockApplicationMasterProtocol(schedHost,schedPort,tokenService, sessionConfig,rpcAuth);

		RegisterApplicationMasterResponseProto responseProto;
		EXPECT_CALL((*protocol),registerApplicationMaster(_)).Times(AnyNumber()).WillOnce(Return(RegisterApplicationMasterResponse(responseProto)));

		AllocateResponseProto allocateResponseProto;
		EXPECT_CALL((*protocol),allocate(_)).Times(AnyNumber()).WillOnce(Return(AllocateResponse(allocateResponseProto)));

		FinishApplicationMasterResponseProto finishApplicationMasterResponseProto;
		EXPECT_CALL((*protocol),finishApplicationMaster(_)).Times(AnyNumber()).WillOnce(Return(FinishApplicationMasterResponse(finishApplicationMasterResponseProto)));

		client = new ApplicationMaster(protocol);
	}
	~TestApplicationMaster(){
		delete client;
	}
protected:
	ApplicationMaster *client;
};

TEST_F(TestApplicationMaster,TestRegisterApplicationMaster){
	string amHost("localhost");
	int amPort = 8032;
	string am_tracking_url = "";
	RegisterApplicationMasterResponse response = client->registerApplicationMaster(amHost,amPort,am_tracking_url);
	EXPECT_EQ(response.getProto()._cached_size_,0);
}

TEST_F(TestApplicationMaster,TestAllocate){
	list<ResourceRequest> asks;
	list<ContainerId> releases;
	ResourceBlacklistRequest blacklistRequest;
	int32_t responseId;
	float progress = 5;
	AllocateResponse response = client->allocate(asks,releases,blacklistRequest,responseId,progress);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestApplicationMaster,TestFinishApplicationMaster){
	string diagnostics("");
	string trackingUrl("");
	FinalApplicationStatus finalstatus;
	bool response = client->finishApplicationMaster(diagnostics,trackingUrl,finalstatus);
	EXPECT_EQ(response,false);
}



