/*
 * TestLibYarnClient.cpp
 *
 *  Created on: Mar 2, 2015
 *      Author: weikui
 */
#include "gtest/gtest.h"
#include "libyarnclient/LibYarnClient.h"
#include "records/FinalApplicationStatus.h"

using namespace std;
using namespace libyarn;

class TestLibYarnClient: public ::testing::Test {
public:
	TestLibYarnClient(){
		string rmHost("localhost");
		string rmPort("9980");
		string schedHost("localhost");
		string schedPort("9981");
		string amHost("localhost");
		int32_t amPort = 0;
		string am_tracking_url("url");
		int heartbeatInterval = 1000;
		client = new LibYarnClient(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval);
	}
	~TestLibYarnClient(){
	}
protected:
	LibYarnClient *client;
};

TEST_F(TestLibYarnClient,TestLibYarn){
	string jobName("libyarn");
	string queue("sample_queue");
	string jobId("");
	int result = client->createJob(jobName, queue,jobId);
	EXPECT_EQ(result,0);

	ResourceRequest resRequest;
	string host("*");
	resRequest.setResourceName(host);
	Resource capability;
	capability.setVirtualCores(1);
	capability.setMemory(1024);
	resRequest.setCapability(capability);
	resRequest.setNumContainers(3);
	resRequest.setRelaxLocality(true);
	Priority priority;
	priority.setPriority(1);
	resRequest.setPriority(priority);
	list<string> blackListAdditions;
	list<string> blackListRemovals;
	list<Container> allocatedResourcesArray;
	result = client->allocateResources(jobId, resRequest, blackListAdditions, blackListRemovals,allocatedResourcesArray,5);
	EXPECT_EQ(result,0);

	int allocatedResourceArraySize = allocatedResourcesArray.size();
	int activeContainerIds[allocatedResourceArraySize];
	int releaseContainerIds[allocatedResourceArraySize];
	int statusContainerIds[allocatedResourceArraySize];
	int i = 0;
	for (list<Container>::iterator it = allocatedResourcesArray.begin();it != allocatedResourcesArray.end();it++){
		activeContainerIds[i] = it->getId().getId();
		releaseContainerIds[i] = it->getId().getId();
		statusContainerIds[i] = it->getId().getId();
		i++;
	}
	result = client->activeResources(jobId, activeContainerIds,allocatedResourceArraySize);
	EXPECT_EQ(result,0);

	sleep(1);

	set<int> activeFailIds;
	result = client->getActiveFailContainerIds(activeFailIds);
	EXPECT_EQ(result,0);
	EXPECT_EQ(activeFailIds.size(),0);

	ApplicationReport report;
	result = client->getApplicationReport(jobId,report);
	EXPECT_EQ(result,0);

	list<ContainerReport> containerReports;
	result = client->getContainerReports(jobId,containerReports);
	EXPECT_EQ(result,0);

	list<ContainerStatus> containerStatues;
	result = client->getContainerStatuses(jobId,statusContainerIds,allocatedResourceArraySize,containerStatues);
	EXPECT_EQ(result,0);

	result = client->releaseResources(jobId, releaseContainerIds,allocatedResourceArraySize);
	EXPECT_EQ(result,0);

	QueueInfo queueInfo;
	result = client->getQueueInfo(queue, true, true, true, queueInfo);
	EXPECT_EQ(result,0);

	list<NodeReport> nodeReports;
	list<NodeState> nodeStates;
	nodeStates.push_back(NodeState::NS_RUNNING);
	result = client->getClusterNodes(nodeStates,nodeReports);
	EXPECT_EQ(result,0);

	result = client->finishJob(jobId, FinalApplicationStatus::APP_SUCCEEDED);
	EXPECT_EQ(result,0);
}






