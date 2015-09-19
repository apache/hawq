/*
 * TestMockApplicationMasterProtocol.cpp
 *
 *  Created on: Mar 9, 2015
 *      Author: weikui
 */

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "MockApplicationMasterProtocol.h"

using namespace testing;
using namespace Mock;
using namespace std;

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




