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

#include <list>
#include <map>
#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "libyarnclient/ContainerManagement.h"
#include "MockContainerManagementProtocol.h"
#include "TestContainerManagementStub.h"

using std::map;
using std::string;
using std::list;
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

	StringBytesMap map;
	string key("key");
	string value("value");
	map.setKey(key);
	map.setValue(value);
	list<StringBytesMap> maps;
	maps.push_back(map);
	ContainerId containerId;
	containerId.setId(501);
	list<ContainerId> containerIds;
	containerIds.push_back(containerId);
	ContainerExceptionMap exceptionMap;
	exceptionMap.setContainerId(containerId);
	SerializedException exception;
	string message("message");
	string trace("trace");
	string className("className");
	exception.setMessage(message);
	exception.setTrace(trace);
	exception.setClassName(className);
	SerializedException cause;
	string message2("message2");
	cause.setMessage(message2);
	exception.setCause(cause);
	exceptionMap.setSerializedException(exception);
	list<ContainerExceptionMap> exceptionMaps;
	exceptionMaps.push_back(exceptionMap);
	StartContainersResponse response;
	response.setServicesMetaData(maps);
	response.setSucceededRequests(containerIds);
	response.setFailedRequests(exceptionMaps);
	EXPECT_CALL(*protocol, startContainers(_)).Times(AnyNumber()).WillOnce(Return(response));
	client.stub = &stub;
	EXPECT_CALL(stub, getContainerManagementProtocol()).Times(AnyNumber()).WillOnce(Return(protocol));

	Container container;
	StartContainerRequest request;
	libyarn::Token nmToken;
	StartContainerResponse ret = client.startContainer(container,request,nmToken);
	list<StringBytesMap>::iterator itMap = ret.getServicesMetaData().begin();
	//EXPECT_EQ(itMap->getKey(), "key");
	//EXPECT_EQ(itMap->getValue(), "value");
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

	GetContainerStatusesResponse getResponse;
	ContainerId containerId;
	containerId.setId(501);
	ContainerStatus status;
	status.setContainerId(containerId);
	list<ContainerStatus> statuses;
	statuses.push_back(status);
	getResponse.setContainerStatuses(statuses);
	EXPECT_CALL(*protocol, getContainerStatuses(_)).Times(1).WillOnce(Return(getResponse));
	client.stub = &stub;
	EXPECT_CALL(stub, getContainerManagementProtocol()).Times(1).WillRepeatedly(Return(protocol));

	Container container;
	libyarn::Token nmToken;
	ContainerStatus retStatus = client.getContainerStatus(container,nmToken);
	EXPECT_EQ(retStatus.getContainerId().getId(), 501);

	GetContainerStatusesResponse getEmptyResponse;
	protocol =new MockContainerManagementProtocol(nmHost,nmPort,tokenService,sessionConfig,rpcAuth);
	EXPECT_CALL(*protocol, getContainerStatuses(_)).Times(1).WillOnce(Return(getEmptyResponse));
	EXPECT_CALL(stub, getContainerManagementProtocol()).Times(1).WillRepeatedly(Return(protocol));

	retStatus = client.getContainerStatus(container, nmToken);
	EXPECT_EQ(retStatus.getContainerId().getId(), 0);
}

