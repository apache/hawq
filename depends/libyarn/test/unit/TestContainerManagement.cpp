/*
 * TestContainerManagement.cpp
 *
 *  Created on: Mar 9, 2015
 *      Author: weikui
 */

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "libyarnclient/ContainerManagement.h"
#include "MockContainerManagementProtocol.h"
#include "TestContainerManagementStub.h"

using namespace std;
using namespace libyarn;
using namespace testing;
using namespace Mock;

class MockContainerManagementStub: public TestContainerManagementStub {
public:
    MOCK_METHOD0(getContainerManagementProtocol, ContainerManagementProtocol * ());
};

TEST(TestContainerManagement,TestStartContainer){
	ContainerManagement client;
	MockContainerManagementStub stub;
	string nmHost("localhost");
	string nmPort("8032");
	string tokenService = "";
	Yarn::Config config;
	Yarn::Internal::SessionConfig sessionConfig(config);
	Yarn::Internal::UserInfo user = Yarn::Internal::UserInfo::LocalUser();
	Yarn::Internal::RpcAuth rpcAuth(user, Yarn::Internal::AuthMethod::SIMPLE);
	MockContainerManagementProtocol *protocol =new MockContainerManagementProtocol(nmHost,nmPort,tokenService,sessionConfig,rpcAuth);
	StartContainersResponseProto responseProto;
	EXPECT_CALL(*protocol, startContainers(_)).Times(AnyNumber()).WillOnce(Return(StartContainersResponse(responseProto)));
	client.stub = &stub;
	EXPECT_CALL(stub, getContainerManagementProtocol()).Times(AnyNumber()).WillOnce(Return(protocol));

	Container container;
	StartContainerRequest request;
	libyarn::Token nmToken;
	client.startContainer(container,request,nmToken);
}

TEST(TestContainerManagement,TestStopContainer){
	ContainerManagement client;
	MockContainerManagementStub stub;
	string nmHost("localhost");
	string nmPort("8032");
	string tokenService = "";
	Yarn::Config config;
	Yarn::Internal::SessionConfig sessionConfig(config);
	Yarn::Internal::UserInfo user = Yarn::Internal::UserInfo::LocalUser();
	Yarn::Internal::RpcAuth rpcAuth(user, Yarn::Internal::AuthMethod::SIMPLE);
	MockContainerManagementProtocol *protocol =new MockContainerManagementProtocol(nmHost,nmPort,tokenService,sessionConfig,rpcAuth);
	StopContainersResponseProto stopResponseProto;
	EXPECT_CALL(*protocol, stopContainers(_)).Times(AnyNumber()).WillOnce(Return(StopContainersResponse(stopResponseProto)));
	client.stub = &stub;
	EXPECT_CALL(stub, getContainerManagementProtocol()).Times(AnyNumber()).WillOnce(Return(protocol));

	Container container;
	libyarn::Token nmToken;
	client.stopContainer(container,nmToken);
}

TEST(TestContainerManagement,TestGetContainerStatus){
	ContainerManagement client;
	MockContainerManagementStub stub;
	string nmHost("localhost");
	string nmPort("8032");
	string tokenService = "";
	Yarn::Config config;
	Yarn::Internal::SessionConfig sessionConfig(config);
	Yarn::Internal::UserInfo user = Yarn::Internal::UserInfo::LocalUser();
	Yarn::Internal::RpcAuth rpcAuth(user, Yarn::Internal::AuthMethod::SIMPLE);
	MockContainerManagementProtocol *protocol =new MockContainerManagementProtocol(nmHost,nmPort,tokenService,sessionConfig,rpcAuth);
	GetContainerStatusesResponseProto getResponseProto;
	EXPECT_CALL(*protocol, getContainerStatuses(_)).Times(AnyNumber()).WillOnce(Return(GetContainerStatusesResponse(getResponseProto)));
	client.stub = &stub;
	EXPECT_CALL(stub, getContainerManagementProtocol()).Times(AnyNumber()).WillOnce(Return(protocol));

	Container container;
	libyarn::Token nmToken;
	ContainerStatus status = client.getContainerStatus(container,nmToken);
	EXPECT_EQ(status.getContainerId().getId(),0);
}

