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
#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "libyarnclient/ApplicationMaster.h"
#include "MockApplicationMasterProtocol.h"

using std::string;
using std::list;
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
		protocol = new MockApplicationMasterProtocol(schedHost,schedPort,tokenService, sessionConfig,rpcAuth);
		client = new ApplicationMaster(protocol);
	}
	~TestApplicationMaster(){
		delete client;
	}

protected:
	MockApplicationMasterProtocol *protocol;
	ApplicationMaster *client;
};

TEST_F(TestApplicationMaster,TestRegisterApplicationMaster){
	Resource resource;
	resource.setMemory(1024*8*10);
	resource.setVirtualCores(1*8*10);
	string key("tokenkey");
	ApplicationACLMap aclMap;
	aclMap.setAccessType(ApplicationAccessType::APPACCESS_VIEW_APP);
	string acl("acl");
	aclMap.setAcl(acl);
	list<ApplicationACLMap> aclMapList;
	aclMapList.push_back(aclMap);
	RegisterApplicationMasterResponse response;
	response.setMaximumResourceCapability(resource);
	response.setClientToAMTokenMasterKey(key);
	response.setApplicationACLs(aclMapList);
	EXPECT_CALL((*protocol),registerApplicationMaster(_)).Times(AnyNumber()).WillOnce(Return(response));

	string amHost("localhost");
	int amPort = 8032;
	string am_tracking_url = "";
	RegisterApplicationMasterResponse retResponse = client->registerApplicationMaster(amHost,amPort,am_tracking_url);
	EXPECT_EQ(retResponse.getClientToAMTokenMasterKey(), "tokenkey");
	Resource retResource = retResponse.getMaximumResourceCapability();
	EXPECT_EQ(retResource.getMemory(), 1024*8*10);
	EXPECT_EQ(retResource.getVirtualCores(), 1*8*10);
	list<ApplicationACLMap> retAclMapList = retResponse.getApplicationACLs();
	EXPECT_EQ(retAclMapList.size(), 1);
	list<ApplicationACLMap>::iterator it = retAclMapList.begin();
	EXPECT_EQ(it->getAccessType(), ApplicationAccessType::APPACCESS_VIEW_APP);
	EXPECT_EQ(it->getAcl(), "acl");
}

TEST_F(TestApplicationMaster,TestAllocate){
	Resource resource;
	resource.setMemory(1024*8*10);
	resource.setVirtualCores(1*8*10);
	AllocateResponse allocateResponse;
	allocateResponse.setAMCommand(AMCommand::AM_RESYNC);
	allocateResponse.setResponseId(100);
	list<Container> containers;
	Container container;
	ContainerId containerId;
	containerId.setId(501);
	container.setId(containerId);
	NodeId nodeId;
	string nodeHost("node1");
	nodeId.setHost(nodeHost);
	nodeId.setPort(9983);
	container.setNodeId(nodeId);
	string address("http://address");
	container.setNodeHttpAddress(address);
	container.setResource(resource);
	Priority priority;
	priority.setPriority(1);
	container.setPriority(priority);
	libyarn::Token token;
	string identifier("identifier");
	token.setIdentifier(identifier);
	string password("password");
	token.setPassword(password);
	string kind("kind");
	token.setKind(kind);
	string service("service");
	token.setService(service);
	container.setContainerToken(token);
	containers.push_back(container);
	allocateResponse.setAllocatedContainers(containers);
	ContainerStatus containerStatus;
	containerStatus.setContainerId(containerId);
	containerStatus.setContainerState(ContainerState::C_RUNNING);
	string diagnostics("diagnostics");
	containerStatus.setDiagnostics(diagnostics);
	containerStatus.setExitStatus(-1000);
	list<ContainerStatus> statuses;
	statuses.push_back(containerStatus);
	allocateResponse.setCompletedContainerStatuses(statuses);
	allocateResponse.setResourceLimit(resource);
	NodeReport nodeReport;
	nodeReport.setNodeId(nodeId);
	string rackName("default-rack");
	nodeReport.setRackName(rackName);
	nodeReport.setNumContainers(8);
	list<NodeReport> nodeReports;
	nodeReports.push_back(nodeReport);
	allocateResponse.setUpdatedNodes(nodeReports);
	allocateResponse.setNumClusterNodes(12);
	NMToken nmToken;
	nmToken.setNodeId(nodeId);
	nmToken.setToken(token);
	list<NMToken> nmTokens;
	nmTokens.push_back(nmToken);
	allocateResponse.setNMTokens(nmTokens);
	EXPECT_CALL((*protocol),allocate(_)).Times(AnyNumber()).WillOnce(Return(allocateResponse));

	list<ResourceRequest> asks;
	list<ContainerId> releases;
	ResourceBlacklistRequest blacklistRequest;
	int32_t responseId;
	float progress = 5;
	AllocateResponse retResponse = client->allocate(asks,releases,blacklistRequest,responseId,progress);
	EXPECT_EQ(retResponse.getAMCommand(), AMCommand::AM_RESYNC);
	EXPECT_EQ(retResponse.getResponseId(), 100);
	list<Container> retContainers = retResponse.getAllocatedContainers();
	list<Container>::iterator it = retContainers.begin();
	EXPECT_EQ(it->getId().getId(), 501);
	EXPECT_EQ(it->getNodeId().getHost(), "node1");
	EXPECT_EQ(it->getNodeId().getPort(), 9983);
	EXPECT_EQ(it->getNodeHttpAddress(), "http://address");
	EXPECT_EQ(it->getPriority().getPriority(), 1);
	EXPECT_EQ(it->getResource().getMemory(), 1024*8*10);
	EXPECT_EQ(it->getResource().getVirtualCores(), 1*8*10);
	EXPECT_EQ(it->getContainerToken().getIdentifier(), "identifier");
	EXPECT_EQ(it->getContainerToken().getPassword(), "password");
	EXPECT_EQ(it->getContainerToken().getKind(), "kind");
	EXPECT_EQ(it->getContainerToken().getService(), "service");
	list<ContainerStatus>::iterator retStatus = retResponse.getCompletedContainersStatuses().begin();
	EXPECT_EQ(retStatus->getContainerId().getId(), 501);
	EXPECT_EQ(retStatus->getContainerState(), ContainerState::C_RUNNING);
	//EXPECT_EQ(retStatus->getDiagnostics(), "diagnostics");
	EXPECT_EQ(retStatus->getExitStatus(), -1000);
	EXPECT_EQ(retResponse.getResourceLimit().getMemory(), 1024*8*10);
	//list<NodeReport>::iterator report = response.getUpdatedNodes().begin();
	//EXPECT_EQ(report->getNodeId().getHost(), "node1");
	//list<NMToken>::iterator nmToken = response.getNMTokens().begin();
	//EXPECT_EQ(nmToken->getNodeId().getHost(), "node1");
	//EXPECT_EQ(nmToken->getToken().getIdentifier(), "identifier");
	EXPECT_EQ(retResponse.getNumClusterNodes(), 12);
}

TEST_F(TestApplicationMaster,TestFinishApplicationMaster){
	FinishApplicationMasterResponse finishApplicationMasterResponse;
	finishApplicationMasterResponse.setIsUnregistered(true);
	EXPECT_CALL((*protocol),finishApplicationMaster(_)).Times(AnyNumber()).WillOnce(Return(finishApplicationMasterResponse));
	string diagnostics("");
	string trackingUrl("");
	FinalApplicationStatus finalstatus;
	bool response = client->finishApplicationMaster(diagnostics,trackingUrl,finalstatus);
	EXPECT_EQ(response,true);
}

