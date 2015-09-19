/*
 * TestLibYarnClient.cpp
 *
 *  Created on: Mar 11, 2015
 *      Author: weikui
 */

#include "gtest/gtest.h"
#include "gmock/gmock.h"
//#include "Thread.h"

#include "libyarnclient/LibYarnClient.h"
#include "MockApplicationMaster.h"
#include "MockApplicationClient.h"
#include "MockContainerManagement.h"
#include "TestLibYarnClientStub.h"

using namespace std;
using namespace libyarn;
using namespace testing;
using namespace Mock;

class MockLibYarnClientStub: public TestLibYarnClientStub {
public:
	~MockLibYarnClientStub(){
	}
	MOCK_METHOD0(getApplicationClient, ApplicationClient * ());
	MOCK_METHOD0(getApplicationMaster, ApplicationMaster * ());
    MOCK_METHOD0(getContainerManagement, ContainerManagement * ());
};

class TestLibYarnClient: public ::testing::Test {
public:
	TestLibYarnClient(){
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

static list<ContainerReport> BuildContainerReportList(int containerSize){
	list<ContainerReport> containerReports;
	for (int i = 0; i < containerSize; i++) {
		ContainerReport report;
		containerReports.push_back(report);
	}
	return containerReports;
}
static AllocateResponse BuildAllocateResponse(int containerNumber){
	AllocateResponse allocateResponse;
	allocateResponse.setResponseId(10);
	list<Container> containers;
	for (int i=0;i<containerNumber;i++){
		Container container;
		ContainerId containerId;
		containerId.setId(i+1);
		container.setId(containerId);
		containers.push_back(container);
	}
	list<NMToken> nmTokens;
	for (int i=0;i<containerNumber;i++){
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

static ApplicationID BuildApplicationID(int id){
	ApplicationID appId;
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
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationID(1)));
	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());
	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AtLeast(3))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::FAILED)))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::ACCEPTED)))
			.WillRepeatedly(Return(BuildApplicationReport("pass",YarnApplicationState::ACCEPTED)));

	EXPECT_CALL((*amrmclient),registerApplicationMaster(_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildRegisterResponse()));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(0)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName,queue,jobId);
	EXPECT_EQ(result,0);
}

/*TEST_F(TestLibYarnClient,TestCreateJobException){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationID(1)));
	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber())
			.WillOnce(Throw(std::invalid_argument("Exist an application for the client")));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName,queue,jobId);
	EXPECT_EQ(result,1);
}

TEST_F(TestLibYarnClient,TestCreateJobInvalidId){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("test");
	int result = client.createJob(jobName,queue,jobId);
	EXPECT_EQ(result,1);
}
*/

TEST_F(TestLibYarnClient,TestAllocateResources){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(5)));
	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
				.WillRepeatedly(Return(BuildContainerReportList(0)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<string> blackListAdditions;
	list<string> blackListRemovals;
	ResourceRequest resRequest;
	list<Container> allocatedResourcesArray;
	int result;

	/*
	resRequest = BuildRequest(3);
	result = client.allocateResources(jobId, resRequest, blackListAdditions, blackListRemovals,allocatedResourcesArray,5);
	EXPECT_EQ(result,0);
	resRequest = BuildRequest(5);
	result = client.allocateResources(jobId, resRequest, blackListAdditions, blackListRemovals,allocatedResourcesArray,5);
	EXPECT_EQ(result,0);
	resRequest = BuildRequest(8);
	result = client.allocateResources(jobId, resRequest, blackListAdditions, blackListRemovals,allocatedResourcesArray,5);
	EXPECT_EQ(result,0);
	*/
}

TEST_F(TestLibYarnClient,TestAllocateResourcesRetry){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
					.WillRepeatedly(Return(BuildContainerReportList(0)));
	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber())
			.WillOnce(Return(BuildAllocateResponse(5)))
			.WillOnce(Return(BuildAllocateResponse(5)))
			.WillRepeatedly(Return(BuildAllocateResponse(0)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<string> blackListAdditions;
	list<string> blackListRemovals;
	ResourceRequest resRequest;
	list<Container> allocatedResourcesArray;
	int result;
	resRequest = BuildRequest(11);
	//result = client.allocateResources(jobId, resRequest, blackListAdditions, blackListRemovals,allocatedResourcesArray,2);
	//EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestActiveResources){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	int activeContainerSize = 3;
	int activeContainerIds[activeContainerSize];
	for (int i = 0;i < activeContainerSize;i++){
		activeContainerIds[i] = i;
	}
	int result = client.activeResources(jobId,activeContainerIds,activeContainerSize);
	EXPECT_EQ(result,0);
}


TEST_F(TestLibYarnClient,TestReleaseResources){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*amrmclient),allocate(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(BuildAllocateResponse(5)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));

	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	int releaseContainerSize = 3;
	int releaseContainerIds[releaseContainerSize];
	for (int i = 0;i < releaseContainerSize;i++){
		releaseContainerIds[i] = i;
	}
	int result = client.releaseResources(jobId,releaseContainerIds,releaseContainerSize);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestFinishJob){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*amrmclient),finishApplicationMaster(_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(true));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	int result = client.finishJob(jobId,FinalApplicationStatus::APP_SUCCEEDED);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestGetApplicationReport){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AnyNumber())
				.WillRepeatedly(Return(BuildApplicationReport("pass",YarnApplicationState::ACCEPTED)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	ApplicationReport applicationReport;
	int result = client.getApplicationReport(jobId,applicationReport);
	EXPECT_EQ(result,0);
}

TEST_F(TestLibYarnClient,TestGetContainerReports){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	int containerSize = 3;
	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
				.WillRepeatedly(Return(BuildContainerReportList(containerSize)));

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<ContainerReport> reports;
	int result = client.getContainerReports(jobId,reports);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(reports.size()),containerSize);
}

TEST_F(TestLibYarnClient,TestGetContainerStatuses){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(stub, getApplicationClient()).Times(AnyNumber()).WillOnce(Return(appclient));
	EXPECT_CALL(stub, getApplicationMaster()).Times(AnyNumber()).WillOnce(Return(amrmclient));
	EXPECT_CALL(stub, getContainerManagement()).Times(AnyNumber()).WillOnce(Return(nmclient));
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);

	string jobId("");
	list<ContainerStatus> containerStatues;
	int containerSize = 3;
	int containerIds[containerSize];
	for (int i = 0;i < containerSize;i++){
		containerIds[i] = i;
	}
	int result = client.getContainerStatuses(jobId,containerIds,containerSize,containerStatues);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(containerStatues.size()),0);
}

TEST_F(TestLibYarnClient,TestGetQueueInfo){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
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
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);


	QueueInfo resultQueue;
	int result = client.getQueueInfo(queue,true,true,true,resultQueue);
	EXPECT_EQ(result,0);
	EXPECT_EQ(resultQueue.getCurrentCapacity(),capcity);
	EXPECT_STREQ(resultQueue.getQueueName().c_str(),queue.c_str());
	EXPECT_EQ(int(resultQueue.getChildQueues().size()),childNum);
}

TEST_F(TestLibYarnClient,TestGetClusterNodes){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
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
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);


	list<NodeState> states;
	list<NodeReport> nodeResult;
	int result = client.getClusterNodes(states,nodeResult);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(nodeResult.size()),nodeSize);
}

TEST_F(TestLibYarnClient,TestGetErrorMessage){
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval);
	EXPECT_STREQ("",client.getErrorMessage().c_str());
	client.setErrorMessage("error!");
	EXPECT_STREQ("error!",client.getErrorMessage().c_str());
}

TEST_F(TestLibYarnClient,TestGetActiveFailContainerIds){
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval);
	set<int> activeFailIds;
	client.getActiveFailContainerIds(activeFailIds);
	EXPECT_EQ(int(activeFailIds.size()),0);
}

TEST_F(TestLibYarnClient,TestLibYarn){
	MockApplicationClient *appclient = new MockApplicationClient(rmHost,rmHost);
	MockApplicationMaster *amrmclient = new MockApplicationMaster(schedHost,schedPort,user,tokenService);
	MockContainerManagement *nmclient = new MockContainerManagement();
	MockLibYarnClientStub stub;

	EXPECT_CALL(*appclient, getNewApplication()).Times(AnyNumber()).WillOnce(Return(BuildApplicationID(1)));
	EXPECT_CALL((*appclient),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());
	EXPECT_CALL((*appclient),getApplicationReport(_)).Times(AtLeast(3))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::FAILED)))
			.WillOnce(Return(BuildApplicationReport("",YarnApplicationState::ACCEPTED)))
			.WillRepeatedly(Return(BuildApplicationReport("pass",YarnApplicationState::ACCEPTED)));
	EXPECT_CALL((*appclient),getContainers(_)).Times(AnyNumber())
					.WillRepeatedly(Return(BuildContainerReportList(0)));

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

	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval,&stub);
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client.createJob(jobName,queue,jobId);
	EXPECT_EQ(result,0);

	list<string> blackListAdditions;
	list<string> blackListRemovals;
	ResourceRequest resRequest;
	list<Container> allocatedResourcesArray;

	//resRequest = BuildRequest(3);
	//result = client.allocateResources(jobId, resRequest, blackListAdditions, blackListRemovals,allocatedResourcesArray,5);
	EXPECT_EQ(result,0);

	int allocatedResourceArraySize = allocatedResourcesArray.size();
	int activeContainerIds[allocatedResourceArraySize];
	int releaseContainerIds[allocatedResourceArraySize];
	int statusContainerIds[allocatedResourceArraySize];
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

	set<int> activeFailIds;
	result = client.getActiveFailContainerIds(activeFailIds);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(activeFailIds.size()),1);

	ApplicationReport report;
	result = client.getApplicationReport(jobId,report);
	EXPECT_EQ(result,0);

	list<ContainerStatus> containerStatues;
	result = client.getContainerStatuses(jobId,statusContainerIds,allocatedResourceArraySize,containerStatues);
	EXPECT_EQ(result,0);
	EXPECT_EQ(int(containerStatues.size()),2);

	result = client.releaseResources(jobId, releaseContainerIds,allocatedResourceArraySize);
	EXPECT_EQ(result,0);


	result = client.finishJob(jobId, FinalApplicationStatus::APP_SUCCEEDED);
	EXPECT_EQ(result,0);
}


