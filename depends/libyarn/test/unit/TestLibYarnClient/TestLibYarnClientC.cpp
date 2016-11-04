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

#include "libyarnclient/LibYarnClientC.h"
#include "MockLibYarnClient.h"

using std::string;
using namespace libyarn;
using namespace testing;
using namespace Mock;


extern "C" LibYarnClient_t* getLibYarnClientT(LibYarnClient *libyarnClient);

class TestLibYarnClientC: public ::testing::Test {
public:
	TestLibYarnClientC(){
		string amUser("postgres");
		string rmHost("localhost");
		string rmPort("8032");
		string schedHost("localhost");
		string schedPort("8030");
		string amHost("localhost");
		int32_t amPort = 0;
		string am_tracking_url("url");
		int heartbeatInterval = 1000;
		libyarnClient = new MockLibYarnClient(amUser, rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url,heartbeatInterval);
	}
	~TestLibYarnClientC(){
		delete libyarnClient;
	}
protected:
	MockLibYarnClient *libyarnClient;
};

static char* StringToChar(string str){
	char *cstr = new char[str.length()+1];
	strcpy(cstr,str.c_str());
	return cstr;
}

TEST_F(TestLibYarnClientC,TestNewLibYarnClient){
	char *amUser = StringToChar("postgres");
	char *rmHost = StringToChar("localhost");
	char *rmPort = StringToChar("8032");
	char *schedHost = StringToChar("localhost");
	char *schedPort = StringToChar("8030");
	char *amHost = StringToChar("localhost");
	int32_t amPort = 8090;
	char *am_tracking_url = StringToChar("url");
	int heartbeatInterval = 1000;
	LibYarnClient_t *client = NULL;
	int result = newLibYarnClient(amUser, rmHost, rmPort, schedHost, schedPort,
				amHost, amPort, am_tracking_url,&client,heartbeatInterval);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
}

TEST_F(TestLibYarnClientC,TestCreateJob){
	EXPECT_CALL((*libyarnClient),createJob(_,_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);
	char *jobName = StringToChar("libyarn");
	char *queue = StringToChar("default");
	char *jobId = NULL;
	int result = createJob(client, jobName, queue,&jobId);
	EXPECT_STREQ("",(const char *)(jobId));
	EXPECT_EQ(result,FUNCTION_FAILED);

	result = createJob(client, jobName, queue,&jobId);
	EXPECT_STREQ("",(const char *)(jobId));
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
}

TEST_F(TestLibYarnClientC,TestAllocateResources){
	EXPECT_CALL((*libyarnClient),allocateResources(_,_,_,_,_)).Times(AnyNumber())
			.WillOnce(Return(FUNCTION_FAILED))
			.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);
	char *jobId = StringToChar("");
	LibYarnResourceRequest_t resRequest;
	resRequest.priority = 1;
	resRequest.host = StringToChar("*");
	resRequest.vCores = 1;
	resRequest.memory = 1024;
	resRequest.num_containers = 2;
	resRequest.relax_locality = 1;

	int blacklistAddsSize = 3;
	char *blackListAdditions[blacklistAddsSize];
	for (int i = 0;i<blacklistAddsSize;i++){
		blackListAdditions[i] = StringToChar("");
	}
	int blackListRemovalsSize = 3;
	char *blackListRemovals[blackListRemovalsSize];
	for (int i = 0;i<blackListRemovalsSize;i++){
		blackListRemovals[i] = StringToChar("");
	}

	LibYarnResource_t *allocatedResourcesArray;
	int allocatedResourceArraySize;

	int result = allocateResources(client, jobId, 1, 1, 1024, 2, blackListAdditions,
			blacklistAddsSize, blackListRemovals, blackListRemovalsSize, NULL, 0, &allocatedResourcesArray, &allocatedResourceArraySize);
	EXPECT_EQ(result,FUNCTION_FAILED);

	result = allocateResources(client, jobId, 1, 1, 1024, 2, blackListAdditions,
			blacklistAddsSize, blackListRemovals, blackListRemovalsSize, NULL, 0, &allocatedResourcesArray, &allocatedResourceArraySize);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
	EXPECT_EQ(0,allocatedResourceArraySize);
}

TEST_F(TestLibYarnClientC,TestActiveResources){
	EXPECT_CALL((*libyarnClient),activeResources(_,_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	int activeContainerSize = 0;
	int64_t activeContainerIds[activeContainerSize];
	char *jobId = StringToChar("");
	int result = activeResources(client, jobId, activeContainerIds,activeContainerSize);
	EXPECT_EQ(result,FUNCTION_FAILED);
	result = activeResources(client, jobId, activeContainerIds,activeContainerSize);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
}

TEST_F(TestLibYarnClientC,TestReleaseResources){
	EXPECT_CALL((*libyarnClient),releaseResources(_,_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	int releaseContainerSize = 0;
	int64_t releaseContainerIds[releaseContainerSize];
	char *jobId = StringToChar("");
	int result = releaseResources(client, jobId, releaseContainerIds,releaseContainerSize);
	EXPECT_EQ(result,FUNCTION_FAILED);
	result = releaseResources(client, jobId, releaseContainerIds,releaseContainerSize);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
}

TEST_F(TestLibYarnClientC,TestFinishJob){
	EXPECT_CALL((*libyarnClient),finishJob(_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	char *jobId = StringToChar("");
	int result = finishJob(client, jobId, APPLICATION_SUCCEEDED);
	EXPECT_EQ(result,FUNCTION_FAILED);
	result = finishJob(client, jobId, APPLICATION_SUCCEEDED);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
}

TEST_F(TestLibYarnClientC,TestGetActiveFailContainerIds){
	EXPECT_CALL((*libyarnClient),getActiveFailContainerIds(_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	int64_t *activeFailIds;
	int activeFailSize;
	int result = getActiveFailContainerIds(client,&activeFailIds,&activeFailSize);
	EXPECT_EQ(result,FUNCTION_FAILED);

	result = getActiveFailContainerIds(client,&activeFailIds,&activeFailSize);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
	EXPECT_EQ(0,activeFailSize);
}

TEST_F(TestLibYarnClientC,TestGetApplicationReport){
	EXPECT_CALL((*libyarnClient),getApplicationReport(_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	LibYarnApplicationReport_t *applicationReport = NULL;
	char *jobId = StringToChar("");
	int result = getApplicationReport(client, jobId, &applicationReport);
	EXPECT_EQ(result,FUNCTION_FAILED);
	result = getApplicationReport(client, jobId, &applicationReport);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
	EXPECT_EQ(0,applicationReport->appId);
}

TEST_F(TestLibYarnClientC,TestGetContainerReports){
	EXPECT_CALL((*libyarnClient),getContainerReports(_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	char *jobId = StringToChar("");
	LibYarnContainerReport_t *containerReportArray;
	int containerReportArraySize;
	int result = getContainerReports(client, jobId, &containerReportArray,
									&containerReportArraySize);
	EXPECT_EQ(result,FUNCTION_FAILED);
	result = getContainerReports(client, jobId, &containerReportArray,
									&containerReportArraySize);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
	EXPECT_EQ(0,containerReportArraySize);
}

TEST_F(TestLibYarnClientC,TestGetContainerStatuses){
	EXPECT_CALL((*libyarnClient),getContainerStatuses(_,_,_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	char *jobId = StringToChar("");

	int statusContainerSize = 0;
	int64_t statusContainerIds[statusContainerSize];
	LibYarnContainerStatus_t *containerStatusArray;
	int containerStatusArraySize;
	int result = getContainerStatuses(client, jobId, statusContainerIds,
			statusContainerSize, &containerStatusArray,&containerStatusArraySize);
	EXPECT_EQ(result,FUNCTION_FAILED);
	result = getContainerStatuses(client, jobId, statusContainerIds,
				statusContainerSize, &containerStatusArray,&containerStatusArraySize);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
	EXPECT_EQ(0,containerStatusArraySize);
}

TEST_F(TestLibYarnClientC,TestGetQueueInfo){
	EXPECT_CALL((*libyarnClient),getQueueInfo(_,_,_,_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	char *queue = StringToChar("queue");

	LibYarnQueueInfo_t *queueInfo = NULL;
	int result = getQueueInfo(client, queue, true, true, true, &queueInfo);

	EXPECT_EQ(result,FUNCTION_FAILED);
	result = getQueueInfo(client, queue, true, true, true, &queueInfo);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
}

TEST_F(TestLibYarnClientC,TestGetClusterNodes){
	EXPECT_CALL((*libyarnClient),getClusterNodes(_,_)).Times(AnyNumber())
		.WillOnce(Return(FUNCTION_FAILED))
		.WillOnce(Return(FUNCTION_SUCCEEDED));
	LibYarnClient_t *client = getLibYarnClientT(libyarnClient);

	LibYarnNodeReport_t *nodeReportArray;
	int nodeReportArraySize;
	int result = getClusterNodes(client, NODE_STATE_RUNNING, &nodeReportArray, &nodeReportArraySize);
	EXPECT_EQ(result,FUNCTION_FAILED);
	result = getClusterNodes(client, NODE_STATE_RUNNING, &nodeReportArray, &nodeReportArraySize);
	EXPECT_EQ(result,FUNCTION_SUCCEEDED);
}



