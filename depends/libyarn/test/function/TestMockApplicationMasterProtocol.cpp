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

#include "MockApplicationMasterProtocol.h"

using namespace testing;
using namespace Mock;
using std::string;

class TestMockApplicationMasterProtocol: public ::testing::Test {
public:
	TestMockApplicationMasterProtocol(){
		string schedHost("localhost");
		string schedPort("8032");
		string tokenService = "";
		Yarn::Config config;
		Yarn::Internal::SessionConfig sessionConfig(config);
		Yarn::Internal::UserInfo user = Yarn::Internal::UserInfo::LocalUser();
		Yarn::Internal::RpcAuth rpcAuth(user, Yarn::Internal::AuthMethod::SIMPLE);
		protocol = new MockApplicationMasterProtocol(schedHost,schedPort,tokenService, sessionConfig,rpcAuth);
}
	~TestMockApplicationMasterProtocol(){
		delete protocol;
	}
protected:
	MockApplicationMasterProtocol *protocol;
};

TEST_F(TestMockApplicationMasterProtocol,TestRegisterApplicationMaster){
	RegisterApplicationMasterResponseProto responseProto;
	EXPECT_CALL((*protocol),registerApplicationMaster(_)).Times(AnyNumber()).WillOnce(Return(RegisterApplicationMasterResponse(responseProto)));
	RegisterApplicationMasterRequest request;
	RegisterApplicationMasterResponse response = protocol->registerApplicationMaster(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationMasterProtocol,TestAllocate){
	AllocateResponseProto responseProto;
	EXPECT_CALL((*protocol),allocate(_)).Times(AnyNumber()).WillOnce(Return(AllocateResponse(responseProto)));
	AllocateRequest request;
	AllocateResponse response = protocol->allocate(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationMasterProtocol,TestFinishApplicationMaster){
	FinishApplicationMasterResponseProto responseProto;
	EXPECT_CALL((*protocol),finishApplicationMaster(_)).Times(AnyNumber()).WillOnce(Return(FinishApplicationMasterResponse(responseProto)));
	FinishApplicationMasterRequest request;
	FinishApplicationMasterResponse response = protocol->finishApplicationMaster(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}




