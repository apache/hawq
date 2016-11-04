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
#include "libyarnclient/LibYarnClientC.h"

class TestLibYarnClientC: public ::testing::Test {
public:
	TestLibYarnClientC(){
		char *user = "postgres";
		char *rmHost = "localhost";
		char *rmPort = "8032";
		char *schedHost = "localhost";
		char *schedPort = "8030";
		char *amHost = "localhost";
		int32_t amPort = 0;
		char *am_tracking_url = "url";
		int heartbeatInterval = 1000;
		client = NULL;
		result = newLibYarnClient(user, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url, &client, heartbeatInterval);
	}
	~TestLibYarnClientC(){
	}
protected:
	LibYarnClient_t *client;
	int result;
	int i;
};

TEST_F(TestLibYarnClientC,TestLibYarn){
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);

	char *jobName = "libyarn";
	char *queue = "default";
	char *jobId = NULL;
	result = createJob(client, jobName, queue, &jobId);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);

	LibYarnNodeReport_t *nodeReportArray;
	int nodeReportArraySize;
	result = getClusterNodes(client, NODE_STATE_RUNNING, &nodeReportArray, &nodeReportArraySize);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);
	EXPECT_GT(nodeReportArraySize, 0);

	char *localhost = strdup(nodeReportArray[0].host);

	freeMemNodeReportArray(nodeReportArray, nodeReportArraySize);

	char *blackListAdditions[0];
	char *blackListRemovals[0];
	LibYarnNodeInfo_t preferredHosts[1];
	preferredHosts[0].hostname = localhost;
	preferredHosts[0].rackname = "/default-rack";
	preferredHosts[0].num_containers = 2;
	int preferredHostSize = 1;
	LibYarnResource_t *allocatedResourcesArray;
	int allocatedResourcesArraySize;
	result = allocateResources(client, jobId, 1, 1, 1024, 5,
					blackListAdditions, 0, blackListRemovals, 0, preferredHosts, preferredHostSize,
					&allocatedResourcesArray, &allocatedResourcesArraySize);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);

	int64_t activeContainerIds[allocatedResourcesArraySize];
	int64_t releaseContainerIds[allocatedResourcesArraySize];
	int64_t statusContainerIds[allocatedResourcesArraySize];
	for (i = 0 ; i < allocatedResourcesArraySize; i++) {
		activeContainerIds[i] = allocatedResourcesArray[i].containerId;
		releaseContainerIds[i] = allocatedResourcesArray[i].containerId;
		statusContainerIds[i] = allocatedResourcesArray[i].containerId;
	}

	result = activeResources(client, jobId, activeContainerIds, allocatedResourcesArraySize);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);

	freeMemAllocatedResourcesArray(allocatedResourcesArray, allocatedResourcesArraySize);

	sleep(10);

	int64_t *activeFailIds;
	int activeFailSize;
	result = getActiveFailContainerIds(client,&activeFailIds,&activeFailSize);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);
	EXPECT_EQ(activeFailSize, 0);

	LibYarnApplicationReport_t *applicationReport;
	result = getApplicationReport(client, jobId, &applicationReport);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);

	freeApplicationReport(applicationReport);

	LibYarnContainerReport_t *containerReportArray;
	int containerReportArraySize;
	result = getContainerReports(client, jobId, &containerReportArray, &containerReportArraySize);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);
	EXPECT_EQ(containerReportArraySize, 5);

	freeContainerReportArray(containerReportArray, containerReportArraySize);

	LibYarnContainerStatus_t *containerStatusArray;
	int containerStatusArraySize;
	result = getContainerStatuses(client, jobId, statusContainerIds,
					allocatedResourcesArraySize, &containerStatusArray,
					&containerStatusArraySize);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);
	EXPECT_EQ(containerReportArraySize, 5);

	freeContainerStatusArray(containerStatusArray, containerStatusArraySize);

	result = releaseResources(client, jobId, releaseContainerIds, allocatedResourcesArraySize);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);

	LibYarnQueueInfo_t *queueInfo = NULL;
	result = getQueueInfo(client, queue, true, true, true, &queueInfo);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);

	freeMemQueueInfo(queueInfo);

	result = finishJob(client, jobId, APPLICATION_SUCCEEDED);
	EXPECT_EQ(result, FUNCTION_SUCCEEDED);
}
