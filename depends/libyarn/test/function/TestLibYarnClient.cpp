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
#include "libyarnclient/LibYarnClient.h"
#include "records/FinalApplicationStatus.h"

using std::string;
using std::list;
using std::set;
using namespace libyarn;

class TestLibYarnClient: public ::testing::Test {
public:
	TestLibYarnClient(){
		string user_name("postgres");
		string rmHost("localhost");
		string rmPort("8032");
		string schedHost("localhost");
		string schedPort("8030");
		string amHost("localhost");
		int32_t amPort = 0;
		string am_tracking_url("url");
		int heartbeatInterval = 1000;
		client = new LibYarnClient(user_name, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url, heartbeatInterval);
	}
	~TestLibYarnClient(){
	}
protected:
	LibYarnClient *client;
};

TEST_F(TestLibYarnClient,TestLibYarn){
	string jobName("libyarn");
	string queue("default");
	string jobId("");
	int result = client->createJob(jobName, queue,jobId);
	EXPECT_EQ(result,0);

	list<string> blackListAdditions;
	list<string> blackListRemovals;
	list<Container> allocatedResourcesArray;
	result = client->allocateResources(jobId, blackListAdditions, blackListRemovals,allocatedResourcesArray,5);
	EXPECT_EQ(result,0);

	int allocatedResourceArraySize = allocatedResourcesArray.size();
	int64_t activeContainerIds[allocatedResourceArraySize];
	int64_t releaseContainerIds[allocatedResourceArraySize];
	int64_t statusContainerIds[allocatedResourceArraySize];
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

	set<int64_t> activeFailIds;
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






