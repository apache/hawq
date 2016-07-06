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
#include <set>
#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
//#include "Thread.h"

#include "libyarnclient/LibYarnClient.h"
#include "MockApplicationMaster.h"
#include "MockApplicationClient.h"
#include "MockContainerManagement.h"
#include "TestLibYarnClientStub.h"

using std::string;
using std::list;
using std::set;
using std::map;
using std::pair;
using namespace libyarn;
using namespace testing;
using namespace Mock;

class MockLibYarnClientStub: public TestLibYarnClientStub {
public:
	~MockLibYarnClientStub(){
	}
	MOCK_METHOD0(getApplicationClient, ApplicationClient* ());
	MOCK_METHOD0(getApplicationMaster, ApplicationMaster* ());
    MOCK_METHOD0(getContainerManagement, ContainerManagement* ());
};

class TestLibYarnClient: public ::testing::Test {
public:
	TestLibYarnClient(){
		amUser = "postgres";
		rmHost = "localhost";
		rmPort = "8032";
		schedHost = "localhost";
		schedPort = "8030";
		amHost = "localhost";
		amPort = 0;
		am_tracking_url = "url";
		heartbeatInterval = 1000;
		tokenService = "";
		user = Yarn::Internal::UserInfo::LocalUser();
	}
	~TestLibYarnClient(){
	}
protected:
	string amUser;
	string rmHost;
	string rmPort;
	string schedHost;
	string schedPort;
	string amHost;
	int32_t amPort;
	string am_tracking_url;
	int heartbeatInterval;
	string tokenService;
	Yarn::Internal::UserInfo user;
};

static ResourceRequest BuildRequest(int requestContainer) {
	ResourceRequest resRequest;
	string host("*");
	resRequest.setResourceName(host);
	Resource capability;
	capability.setVirtualCores(1);
	capability.setMemory(1024);
	resRequest.setCapability(capability);
	resRequest.setNumContainers(requestContainer);
	resRequest.setRelaxLocality(true);
	Priority priority;
	priority.setPriority(1);
	resRequest.setPriority(priority);
	return resRequest;
}

static list<ContainerReport> BuildContainerReportList(int containerSize, int startPos = 0){
	list<ContainerReport> containerReports;
	for (int i = startPos; i < containerSize + startPos; i++) {
		ContainerReport report;
		ContainerId containerId;
		containerId.setId(i+1);
		report.setId(containerId);
		containerReports.push_back(report);
	}
	return containerReports;
}
static AllocateResponse BuildAllocateResponse(int containerNumber, int startPos = 0){
	AllocateResponse allocateResponse;
	allocateResponse.setResponseId(10);
	list<Container> containers;
	for (int i=startPos;i<containerNumber+startPos;i++){
		Container container;
		ContainerId containerId;
		containerId.setId(i+1);
		container.setId(containerId);
		containers.push_back(container);
	}
	list<NMToken> nmTokens;
	for (int i=startPos;i<containerNumber+startPos;i++){
		NMToken token;
		nmTokens.push_back(token);
	}
	allocateResponse.setAllocatedContainers(containers);
	allocateResponse.setNMTokens(nmTokens);
	return allocateResponse;
}

static ApplicationReport BuildApplicationReport(string passwordStr,YarnApplicationState state){
	ApplicationReport applicationReport;
	libyarn::Token token;
	string password(passwordStr);
	token.setPassword(password);
	applicationReport.setAMRMToken(token);
	applicationReport.setYarnApplicationState(state);
	return applicationReport;
}

static ApplicationId BuildApplicationId(int id){
	ApplicationId appId;
	appId.setId(id);
	return appId;
}

static RegisterApplicationMasterResponse BuildRegisterResponse(){
	RegisterApplicationMasterResponseProto responseProto;
	return RegisterApplicationMasterResponse(responseProto);
}

static StartContainerResponse BuildStartContainerResponse(){
	StartContainerResponseProto responseProto;
	return StartContainerResponse(responseProto);
}

static ContainerStatus BuildContainerStatus(int id){
	ContainerId containerId;
	containerId.setId(id);
	ContainerStatus containerStatus;
	containerStatus.setContainerId(containerId);
	return containerStatus;
}

static QueueInfo BuildQueueinfo(string queue,float capcity,int childNum){
	QueueInfo queueInfo;
	queueInfo.setQueueName(queue);
	queueInfo.setCurrentCapacity(capcity);

	list<QueueInfo> childQueues;
	for (int i=0;i<childNum;i++){
		QueueInfo childQueue;
		string childName("child");
		childQueue.setQueueName(childName);
		childQueue.setCurrentCapacity(capcity);
		childQueues.push_back(childQueue);
	}
	queueInfo.setChildQueues(childQueues);
	return queueInfo;

}

TEST_F(TestLibYarnClient,TestCreateJob){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationId(1)));
	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());
	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AtLeast(3))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::FAILED)))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::ACCEPTED)))
			.WillRepeatedly(Return(BuildApplicationReport("pass",YarnApplicationState::ACCEPTED)));
	EXPECT_CALL((*appclient), getMethod()).Times(AnyNumber()).WillRepeatedly(Return(SIMPLE));

	EXPECT_CALL((*amrmclient),registerApplicationMaster(_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildRegisterResponse()));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(0)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval, &stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName,queue,jobId);
	EXPECT_EQ(result, 0);
	bool bool_result = client.isJobHealthy();
	EXPECT_FALSE(bool_result);
}

TEST_F(TestLibYarnClient,TestCreateJobCanNotRegister){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationId(1)));
	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());
	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AnyNumber())
			.WillRepeatedly(Return(BuildApplicationReport("",YarnApplicationState::FAILED)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval, &stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName, queue, jobId);
	EXPECT_EQ(result, 1);
}

TEST_F(TestLibYarnClient,TestCreateJobException){
	MockApplicationClient *appclient = new MockApplicationClient(amUser, rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationId(1)));

	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber())
			.WillOnce(Throw(YarnNetworkConnectException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName,queue,jobId);
	EXPECT_EQ(result, 1);
}

TEST_F(TestLibYarnClient,TestCreateJobRepeatedCreate){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationId(1)));
	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());
	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AtLeast(3))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::FAILED)))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::ACCEPTED)))
			.WillRepeatedly(Return(BuildApplicationReport("pass",YarnApplicationState::ACCEPTED)));
	EXPECT_CALL((*appclient), getMethod()).Times(AnyNumber()).WillRepeatedly(Return(SIMPLE));

	EXPECT_CALL((*amrmclient),registerApplicationMaster(_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildRegisterResponse()));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(0)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval, &stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName, queue, jobId);
	EXPECT_EQ(result, 0);
	result = client.createJob(jobName, queue, jobId);
	EXPECT_EQ(result, 1);
}

TEST_F(TestLibYarnClient,TestCreateJobWithUnknownMethod){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationId(1)));
	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());
	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AtLeast(3))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::FAILED)))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::ACCEPTED)))
			.WillRepeatedly(Return(BuildApplicationReport("pass",YarnApplicationState::ACCEPTED)));
	EXPECT_CALL((*appclient), getMethod()).Times(AtLeast(2)).WillRepeatedly(Return(UNKNOWN));

	EXPECT_CALL((*amrmclient),registerApplicationMaster(_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildRegisterResponse()));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(0)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval, &stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName,queue,jobId);
	EXPECT_EQ(result, 0);
}

TEST_F(TestLibYarnClient,TestForceKillJob){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*nmclient), stopContainer(_, _)).Times(AnyNumber()).WillRepeatedly(Return());

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	EXPECT_CALL((*appclient), forceKillApplication(_)).Times(AnyNumber()).WillRepeatedly(Return());

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval, &stub);
	map<int64_t, Container *> jobIdContainers;
	Container *container = new Container();
	NodeId nodeId;
	string rmHost("localhost");
	int rmPort = 8030;
	nodeId.setHost(rmHost);
	nodeId.setPort(rmPort);
	container->setNodeId(nodeId);
	jobIdContainers.insert(pair<int64_t, Container *>(0, container));
	client.jobIdContainers = jobIdContainers;

	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.forceKillJob(jobId);
	EXPECT_EQ(result, 0);
}

TEST_F(TestLibYarnClient,TestForceKillJobInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval, &stub);
	string jobId("InvalidId");
	int result = client.forceKillJob(jobId);
	EXPECT_EQ(result, 1);
}

TEST_F(TestLibYarnClient,TestDummyAllocate){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(5)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	client.dummyAllocate();

	SUCCEED();
}

TEST_F(TestLibYarnClient,TestDummyAllocateException){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber())
			.WillRepeatedly(Throw(YarnIOException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	EXPECT_THROW(client.dummyAllocate(), YarnException);
}


TEST_F(TestLibYarnClient,TestAddResourceRequest){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	Resource resource;
	resource.setMemory(1024);
	resource.setVirtualCores(1);
	string host("master");
	client.addResourceRequest(resource, 10, host, 1, true);
	list<ResourceRequest> request = client.getAskRequests();
	EXPECT_EQ(request.size(), 1);
	list<ResourceRequest>::iterator it = request.begin();
	EXPECT_EQ(it->getPriority().getPriority(), 1);
	EXPECT_EQ(it->getNumContainers(), 10);
	EXPECT_EQ(it->getRelaxLocality(), true);
	EXPECT_EQ(it->getResourceName(), host);
}

TEST_F(TestLibYarnClient,TestAddContainerRequests) {
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	Resource resource;
	resource.setMemory(1024);
	resource.setVirtualCores(1);
	list<struct LibYarnNodeInfo> preferred;
	int ret = client.addContainerRequests(jobId, resource, 8, preferred, 1, true);
	EXPECT_EQ(ret, 0);
	list<ResourceRequest> request = client.getAskRequests();
	list<ResourceRequest>::iterator it = request.begin();
	EXPECT_EQ(it->getPriority().getPriority(), 1);
	EXPECT_EQ(it->getNumContainers(), 8);
	EXPECT_EQ(it->getRelaxLocality(), true);
	EXPECT_EQ(it->getResourceName(), "*");
	client.clearAskRequests();
	EXPECT_EQ(client.getAskRequests().size(), 0);

	/* test preferred hosts */
	LibYarnNodeInfo info1("node1", "", 3);
	LibYarnNodeInfo info2("node2", "", 2);
	preferred.push_back(info1);
	preferred.push_back(info2);
	ret = client.addContainerRequests(jobId, resource, 8, preferred, 1, false);
	request = client.getAskRequests();
	for (it = request.begin(); it != request.end(); ++it) {
		if (it->getResourceName() == info1.getHost()) {
			EXPECT_EQ(it->getNumContainers(), 3);
			EXPECT_EQ(it->getRelaxLocality(), true);
		} else if (it->getResourceName() == info2.getHost()) {
			EXPECT_EQ(it->getNumContainers(), 2);
			EXPECT_EQ(it->getRelaxLocality(), true);
		} else if (it->getResourceName() == string("/default-rack")) {
			EXPECT_EQ(it->getNumContainers(), 5);
			EXPECT_EQ(it->getRelaxLocality(), false);
		} else if (it->getResourceName() == string("*")) {
			EXPECT_EQ(it->getNumContainers(), 8);
			EXPECT_EQ(it->getRelaxLocality(), false);
		} else {
			ASSERT_TRUE(false);
		}
		EXPECT_EQ(it->getCapability().getMemory(), 1024);
		EXPECT_EQ(it->getCapability().getVirtualCores(), 1);
	}
}

TEST_F(TestLibYarnClient,TestAddContainerRequestsInvalidId) {
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("InvalidId");
	Resource resource;
	resource.setMemory(1024);
	resource.setVirtualCores(1);
	list<struct LibYarnNodeInfo> preferred;
	int ret = client.addContainerRequests(jobId, resource, 8, preferred, 1, true);
	EXPECT_EQ(ret, 1);
}

TEST_F(TestLibYarnClient,TestAllocateResources){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(4)
			.WillOnce(Return(BuildAllocateResponse(5, 0)))
			.WillOnce(Return(BuildAllocateResponse(0)))
			.WillOnce(Return(BuildAllocateResponse(5, 2)))
			.WillOnce(Return(BuildAllocateResponse(0)));
	EXPECT_CALL((*appclient),getContainers(_)).Times(4)
			.WillOnce(Return(BuildContainerReportList(0)))
			.WillOnce(Return(BuildContainerReportList(5, 0)))
			.WillOnce(Return(BuildContainerReportList(2, 0)))
			.WillOnce(Return(BuildContainerReportList(7, 0)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<string> blackListAdditions;
	list<string> blackListRemovals;
	list<Container> allocatedResourcesArray;
	int result;

	result = client.allocateResources(jobId, blackListAdditions, blackListRemovals,allocatedResourcesArray,2);
	EXPECT_EQ(allocatedResourcesArray.size(), 2);
	EXPECT_EQ(result,0);
	result = client.allocateResources(jobId, blackListAdditions, blackListRemovals, allocatedResourcesArray, 2);
	EXPECT_EQ(allocatedResourcesArray.size(), 2);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestAllocateResourcesInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(amUser, rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser,rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("InvalidId");
	list<string> blackListAdditions;
	list<string> blackListRemovals;
	list<Container> allocatedResourcesArray;
	int result;
	result = client.allocateResources(jobId, blackListAdditions, blackListRemovals, allocatedResourcesArray, 2);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestAllocateResourcesRetry){
	MockApplicationClient *appclient = new MockApplicationClient(amUser, rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
			.WillRepeatedly(Return(BuildContainerReportList(0)));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber())
			.WillOnce(Return(BuildAllocateResponse(0)))
			.WillRepeatedly(Return(BuildAllocateResponse(5)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser,rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<string> blackListAdditions;
	list<string> blackListRemovals;
	list<Container> allocatedResourcesArray;
	int result;
	result = client.allocateResources(jobId, blackListAdditions, blackListRemovals, allocatedResourcesArray, 2);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestAllocateResourcesRetryFailed){
	MockApplicationClient *appclient = new MockApplicationClient(amUser, rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
			.WillRepeatedly(Return(BuildContainerReportList(0)));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber())
			.WillRepeatedly(Return(BuildAllocateResponse(0)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser,rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<string> blackListAdditions;
	list<string> blackListRemovals;
	list<Container> allocatedResourcesArray;
	int result;
	result = client.allocateResources(jobId, blackListAdditions, blackListRemovals, allocatedResourcesArray, 2);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestAllocateResourcesException){
	MockApplicationClient *appclient = new MockApplicationClient(amUser, rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
			.WillRepeatedly(Return(BuildContainerReportList(0)));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(1)
			.WillOnce(Throw(ApplicationMasterNotRegisteredException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser,rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<string> blackListAdditions;
	list<string> blackListRemovals;
	list<Container> allocatedResourcesArray;
	int result;
	result = client.allocateResources(jobId, blackListAdditions, blackListRemovals, allocatedResourcesArray, 2);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestActiveResources){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser,rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	int activeContainerSize = 3;
	int64_t activeContainerIds[activeContainerSize];
	for (int i = 0;i < activeContainerSize;i++){
		activeContainerIds[i] = i;
	}
	int result = client.activeResources(jobId,activeContainerIds,activeContainerSize);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestActiveResourcesInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser,rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("InvalidId");
	int activeContainerSize = 3;
	int64_t activeContainerIds[activeContainerSize];
	for (int i = 0;i < activeContainerSize;i++){
		activeContainerIds[i] = i;
	}
	int result = client.activeResources(jobId,activeContainerIds,activeContainerSize);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestReleaseResources){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(5)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	int releaseContainerSize = 3;
	int64_t releaseContainerIds[releaseContainerSize];
	for (int i = 0;i < releaseContainerSize;i++){
		releaseContainerIds[i] = i;
	}
	int result = client.releaseResources(jobId,releaseContainerIds,releaseContainerSize);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestReleaseResourcesInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("InvalidId");
	int releaseContainerSize = 3;
	int64_t releaseContainerIds[releaseContainerSize];
	for (int i = 0;i < releaseContainerSize;i++){
		releaseContainerIds[i] = i;
	}
	int result = client.releaseResources(jobId,releaseContainerIds,releaseContainerSize);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestFinishJob){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*amrmclient),finishApplicationMaster(_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(true));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	int result = client.finishJob(jobId,FinalApplicationStatus::APP_SUCCEEDED);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestFinishJobInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("InvalidId");
	int result = client.finishJob(jobId,FinalApplicationStatus::APP_SUCCEEDED);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestGetApplicationReport){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AnyNumber())
				.WillRepeatedly(Return(BuildApplicationReport("pass",YarnApplicationState::ACCEPTED)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	ApplicationReport applicationReport;
	int result = client.getApplicationReport(jobId,applicationReport);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestGetApplicationReportInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("InvalidId");
	ApplicationReport applicationReport;
	int result = client.getApplicationReport(jobId,applicationReport);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestGetContainerReports){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	int containerSize = 3;
	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
				.WillRepeatedly(Return(BuildContainerReportList(containerSize)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<ContainerReport> reports;
	int result = client.getContainerReports(jobId,reports);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(reports.size()),containerSize);
}

TEST_F(TestLibYarnClient,TestGetContainerReportsInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("InvalidId");
	list<ContainerReport> reports;
	int result = client.getContainerReports(jobId,reports);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestGetContainerStatuses){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<ContainerStatus> containerStatues;
	int containerSize = 3;
	int64_t containerIds[containerSize];
	for (int i = 0;i < containerSize;i++){
		containerIds[i] = i;
	}
	int result = client.getContainerStatuses(jobId,containerIds,containerSize,containerStatues);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(containerStatues.size()),0);
}

TEST_F(TestLibYarnClient,TestGetContainerStatusesInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("InvalidId");
	list<ContainerStatus> containerStatues;
	int containerSize = 3;
	int64_t containerIds[containerSize];
	for (int i = 0;i < containerSize;i++){
		containerIds[i] = i;
	}
	int result = client.getContainerStatuses(jobId,containerIds,containerSize,containerStatues);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestGetQueueInfo){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	string queue("test");
	int childNum = 2;
	float capcity = 0.5;
	QueueInfo queueInfo = BuildQueueinfo(queue,capcity,childNum);
	EXPECT_CALL((*appclient),getQueueInfo(_,_,_,_)).Times(AnyNumber())
					.WillRepeatedly(Return(queueInfo));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);


	QueueInfo resultQueue;
	int result = client.getQueueInfo(queue,true,true,true,resultQueue);
	EXPECT_EQ(result,0);
	EXPECT_EQ(resultQueue.getCurrentCapacity(),capcity);
	EXPECT_STREQ(resultQueue.getQueueName().c_str(),queue.c_str());
	EXPECT_EQ(int(resultQueue.getChildQueues().size()),childNum);
}

TEST_F(TestLibYarnClient,TestGetQueueInfoException){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*appclient),getQueueInfo(_,_,_,_)).Times(AnyNumber())
			.WillRepeatedly(Throw(YarnIOException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string queue("test");
	QueueInfo resultQueue;
	int result = client.getQueueInfo(queue,true,true,true,resultQueue);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestGetClusterNodes){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	list<NodeReport> nodeReports;
	int nodeSize = 3;
	for (int i=0;i<nodeSize;i++){
		NodeReport report;
		nodeReports.push_back(report);
	}
	EXPECT_CALL((*appclient),getClusterNodes(_)).Times(AnyNumber())
					.WillRepeatedly(Return(nodeReports));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	list<NodeState> states;
	list<NodeReport> nodeResult;
	int result = client.getClusterNodes(states,nodeResult);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(nodeResult.size()),nodeSize);
}

TEST_F(TestLibYarnClient,TestGetClusterNodesException){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*appclient),getClusterNodes(_)).Times(AnyNumber())
			.WillRepeatedly(Throw(YarnIOException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	list<NodeState> states;
	list<NodeReport> nodeResult;
	int result = client.getClusterNodes(states,nodeResult);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestGetErrorMessage){
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval);
	EXPECT_STREQ("",client.getErrorMessage().c_str());
	client.setErrorMessage("error!");
	EXPECT_STREQ("error!",client.getErrorMessage().c_str());
}

TEST_F(TestLibYarnClient,TestGetActiveFailContainerIds){
	LibYarnClient client(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval);
	set<int64_t> activeFailIds;
	client.getActiveFailContainerIds(activeFailIds);
	EXPECT_EQ(int(activeFailIds.size()),0);
}

TEST_F(TestLibYarnClient,TestLibYarn){
	MockApplicationClient *appclient = new MockApplicationClient(amUser,rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationId(1)));
	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());
	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AtLeast(3))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::FAILED)))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::ACCEPTED)))
			.WillRepeatedly(Return(BuildApplicationReport("pass",YarnApplicationState::ACCEPTED)));
	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
					.WillRepeatedly(Return(BuildContainerReportList(0)));
	EXPECT_CALL((*appclient), getMethod()).Times(AnyNumber()).WillRepeatedly(Return(SIMPLE));

	EXPECT_CALL((*amrmclient),registerApplicationMaster(_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildRegisterResponse()));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(5)));
	EXPECT_CALL((*amrmclient),finishApplicationMaster(_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(true));

	EXPECT_CALL((*nmclient),startContainer(_,_,_)).Times(AnyNumber())
			.WillOnce(Throw(std::invalid_argument("startContainer Exception")))
			.WillRepeatedly(Return(BuildStartContainerResponse()));
	EXPECT_CALL((*nmclient),getContainerStatus(_,_)).Times(AnyNumber())
				.WillOnce(Return(BuildContainerStatus(2)))
				.WillOnce(Return(BuildContainerStatus(3)))
				.WillRepeatedly(Return(BuildContainerStatus(0)));
	EXPECT_CALL((*nmclient),stopContainer(_,_)).Times(AnyNumber())
					.WillRepeatedly(Return());

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(amUser,rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName,queue,jobId);
	EXPECT_EQ(result,0);

	list<string> blackListAdditions;
	list<string> blackListRemovals;
	ResourceRequest resRequest;
	list<Container> allocatedResourcesArray;

	resRequest = BuildRequest(3);
	result = client.allocateResources(jobId, blackListAdditions, blackListRemovals,allocatedResourcesArray,5);
	EXPECT_EQ(result,0);

	int allocatedResourceArraySize = allocatedResourcesArray.size();
	int64_t activeContainerIds[allocatedResourceArraySize];
	int64_t releaseContainerIds[allocatedResourceArraySize];
	int64_t statusContainerIds[allocatedResourceArraySize];
	int i = 0;
	for (list<Container>::iterator it = allocatedResourcesArray.begin();it != allocatedResourcesArray.end();it++){
		activeContainerIds[i] = it->getId().getId();
		if (i != 1){
			releaseContainerIds[i] = it->getId().getId();
		}
		statusContainerIds[i] = it->getId().getId();
		i++;
	}
	result = client.activeResources(jobId, activeContainerIds,allocatedResourceArraySize);
	EXPECT_EQ(result,0);

	set<int64_t> activeFailIds;
	result = client.getActiveFailContainerIds(activeFailIds);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(activeFailIds.size()),1);

	ApplicationReport report;
	result = client.getApplicationReport(jobId,report);
	EXPECT_EQ(result,0);

	list<ContainerStatus> containerStatues;
	result = client.getContainerStatuses(jobId,statusContainerIds,allocatedResourceArraySize,containerStatues);
	EXPECT_EQ(result,0);
	//EXPECT_EQ(int(containerStatues.size()),2);

	result = client.releaseResources(jobId, releaseContainerIds,allocatedResourceArraySize);
	EXPECT_EQ(result,0);


	result = client.finishJob(jobId, FinalApplicationStatus::APP_SUCCEEDED);
	EXPECT_EQ(result,0);
}

