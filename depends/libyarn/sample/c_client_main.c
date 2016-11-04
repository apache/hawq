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

#include <unistd.h>
#include <stdio.h>
#include <sys/time.h>
#include "libyarn/LibYarnClientC.h"
#include "libyarn/LibYarnConstants.h"

int main() {
		char *rmHost = "master";
		char *rmPort = "8032";
		char *schedHost = "master";
		char *schedPort = "8030";
		char *amHost = "master";
		int32_t amPort = 8090;
		char *am_tracking_url = "url";
		int heartbeatInterval = 1000000;
		int i=0;

		//0. new client
		LibYarnClient_t *client = NULL;
		int result = newLibYarnClient("postgres", rmHost, rmPort, schedHost, schedPort,
						amHost, amPort, am_tracking_url,&client,heartbeatInterval);
		printf("newLibYarnClient Result Code:%d\n",result);

		//1. createJob
		char *jobName = "libyarn";
		char *queue = "default";
		char *jobId = NULL;
		result = createJob(client, jobName, queue,&jobId);
		printf("1. createJob, jobid:%s The createJob Result Code:%d\n", jobId,result);
		if (result != FUNCTION_SUCCEEDED){
				const char* errorMessage = getErrorMessage();
				printf("1. createJob, errorMessage:%s\n",errorMessage);
		}


		char *blackListAdditions[0];
		char *blackListRemovals[0];

		//1. allocate resource
		LibYarnResource_t *allocatedResourcesArray;
		int allocatedResourceArraySize;
		result = allocateResources(client, jobId, 1, 1, 1024, 5,
								blackListAdditions, 0, blackListRemovals, 0, NULL, 0,
								&allocatedResourcesArray, &allocatedResourceArraySize);
		printf("The allocateResources Result Code:%d\n",result);
		if (result != FUNCTION_SUCCEEDED){
				const char* errorMessage = getErrorMessage();
				printf("2. allocateResources, errorMessage:%s\n",errorMessage);
		}

		int64_t activeContainerIds[allocatedResourceArraySize];
		int64_t releaseContainerIds[allocatedResourceArraySize];
		int64_t statusContainerIds[allocatedResourceArraySize];
		printf("2. allocateResources, allocatedResourceArraySize:%d\n", allocatedResourceArraySize);
		for (i = 0 ; i < allocatedResourceArraySize; i++) {
			puts("----------------------------");
			printf("allocatedResourcesArray[i].containerId:%ld\n", allocatedResourcesArray[i].containerId);
			activeContainerIds[i] = allocatedResourcesArray[i].containerId;
			releaseContainerIds[i] = allocatedResourcesArray[i].containerId;
			statusContainerIds[i] = allocatedResourcesArray[i].containerId;
			printf("allocatedResourcesArray[i].host:%s\n", allocatedResourcesArray[i].host);
			printf("allocatedResourcesArray[i].port:%d\n", allocatedResourcesArray[i].port);
			printf("allocatedResourcesArray[i].nodeHttpAddress:%s\n", allocatedResourcesArray[i].nodeHttpAddress);
			printf("allocatedResourcesArray[i].vCores:%d\n", allocatedResourcesArray[i].vCores);
			printf("allocatedResourcesArray[i].memory:%d\n", allocatedResourcesArray[i].memory);
		}

		//3. active
		result = activeResources(client, jobId, activeContainerIds,allocatedResourceArraySize);
		printf("The activeResources  Result Code:%d\n",result);
		if (result != FUNCTION_SUCCEEDED){
				const char* errorMessage = getErrorMessage();
				printf("2. activeResources, errorMessage:%s\n",errorMessage);
		}

		sleep(10);

		int64_t *activeFailIds;
		int activeFailSize;
		result = getActiveFailContainerIds(client,&activeFailIds,&activeFailSize);
		printf("Active Fail Container Size:%d\n",activeFailSize);
		for (i = 0;i < activeFailSize;i++){
				printf("Active Fail Container Id:%ld\n",activeFailIds[i]);
		}

		//4. getContainerReport
		LibYarnContainerReport_t *containerReportArray;
		int containerReportArraySize;
		result = getContainerReports(client, jobId, &containerReportArray,
				&containerReportArraySize);
		printf("The getContainerReports Result Code:%d\n", result);
		if (result != FUNCTION_SUCCEEDED) {
			const char* errorMessage = getErrorMessage();
			printf(" The getContainerReports, errorMessage:%s\n", errorMessage);
		}

		printf("containerReportArraySize=%d\n", containerReportArraySize);
		for (i = 0; i < containerReportArraySize; i++) {
			printf("-------------container: %d--------------------------\n", i);
			printf("containerId:%ld\n", containerReportArray[i].containerId);
			printf("vCores:%d\n", containerReportArray[i].vCores);
			printf("memory:%d\n", containerReportArray[i].memory);
			printf("host:%s\n", containerReportArray[i].host);
			printf("port:%d\n", containerReportArray[i].port);
			printf("exitStatus:%d\n", containerReportArray[i].exitStatus);
			printf("state:%d\n", containerReportArray[i].state);
		}
		freeContainerReportArray(containerReportArray, containerReportArraySize);

		//4. getContainerReport
		LibYarnContainerStatus_t *containerStatusArray;
		int containerStatusArraySize;
		result = getContainerStatuses(client, jobId, statusContainerIds,
				allocatedResourceArraySize, &containerStatusArray,
				&containerStatusArraySize);
		printf("The getContainerStatus Result Code:%d\n", result);
		if (result != FUNCTION_SUCCEEDED) {
			const char* errorMessage = getErrorMessage();
			printf(" The getContainerStatus, errorMessage:%s\n", errorMessage);
		}

		printf("containerStatusArraySize=%d\n", containerStatusArraySize);
		for (i = 0; i < containerStatusArraySize; i++) {
			printf("-------------container: %d--------------------------\n", i);
			printf("containerId:%ld\n", containerStatusArray[i].containerId);
			printf("exitStatus:%d\n", containerStatusArray[i].exitStatus);
			printf("state:%d\n", containerStatusArray[i].state);
			printf("diagnostics:%s\n", containerStatusArray[i].diagnostics);
		}
		freeContainerStatusArray(containerStatusArray,containerStatusArraySize);

		//6. getQueueInfo
		LibYarnQueueInfo_t *queueInfo = NULL;
		result = getQueueInfo(client, queue, true, true, true, &queueInfo);
		printf("The getQueueInfo  Result Code:%d\n",result);
		if (result != FUNCTION_SUCCEEDED){
				const char* errorMessage = getErrorMessage();
				printf(" The getQueueInfo, errorMessage:%s\n",errorMessage);
		}

		printf("QueueInfo: queueInfo->queueName:%s, queueInfo->capacity:%f, queueInfo->maximumCapacity:%f, queueInfo->currentCapacity:%f, queueInfo->state:%d\n",
				queueInfo->queueName, queueInfo->capacity, queueInfo->maximumCapacity,
				queueInfo->currentCapacity, queueInfo->state);
		puts("---------chilldQueue:");
		for (i = 0; i < queueInfo->childQueueNameArraySize; i++) {
				printf("QueueInfo: queueInfo->childQueueNameArray[%d]:%s\n", i, queueInfo->childQueueNameArray[i]);
		}
		freeMemQueueInfo(queueInfo);

		//7. getCluster
		LibYarnNodeReport_t *nodeReportArray;
		int nodeReportArraySize;
		result = getClusterNodes(client, NODE_STATE_RUNNING, &nodeReportArray, &nodeReportArraySize);
		printf("The getClusterNodes Result Code:%d\n",result);
		if (result != FUNCTION_SUCCEEDED){
				const char* errorMessage = getErrorMessage();
				printf(" The getClusterNodes, errorMessage:%s\n",errorMessage);
		}

		printf("nodeReportArraySize=%d\n", nodeReportArraySize);
		for (i = 0; i < nodeReportArraySize; i++) {
			printf("-------------node %d--------------------------\n", i);
			printf("host:%s\n", nodeReportArray[i].host);
			printf("port:%d\n", nodeReportArray[i].port);
			printf("httpAddress:%s\n", nodeReportArray[i].httpAddress);
			printf("rackName:%s\n", nodeReportArray[i].rackName);
			printf("memoryUsed:%d\n", nodeReportArray[i].memoryUsed);
			printf("vcoresUsed:%d\n", nodeReportArray[i].vcoresUsed);
			printf("memoryCapability:%d\n", nodeReportArray[i].memoryCapability);
			printf("vcoresCapability:%d\n", nodeReportArray[i].vcoresCapability);
			printf("numContainers:%d\n", nodeReportArray[i].numContainers);
			printf("nodeState:%d\n", nodeReportArray[i].nodeState);
			printf("healthReport:%s\n", nodeReportArray[i].healthReport);
			printf("lastHealthReportTime:%lld\n", nodeReportArray[i].lastHealthReportTime);
		}
		freeMemNodeReportArray(nodeReportArray, nodeReportArraySize);

		//8. getApplicationReport
		LibYarnApplicationReport_t *applicationReport = NULL;
		result = getApplicationReport(client, jobId, &applicationReport);
		printf("The getApplicationReport  Result Code:%d\n", result);
		if (result != FUNCTION_SUCCEEDED) {
			const char* errorMessage = getErrorMessage();
			printf(" The getApplicationReport, errorMessage:%s\n",
					errorMessage);
		}
		printf("-------------ApplicationReport-------------------------\n");
		printf("appId:%d\n", applicationReport->appId);
		printf("user:%s\n",  applicationReport->user);
		printf("queue:%s\n", applicationReport->queue);
		printf("name:%s\n", applicationReport->name);
		printf("host:%s\n", applicationReport->host);
		printf("port:%d\n", applicationReport->port);
		printf("status:%d\n", applicationReport->status);

		freeApplicationReport(applicationReport);

		//5. release
		result = releaseResources(client, jobId, releaseContainerIds,allocatedResourceArraySize);
		printf("The releaseResources  Result Code:%d\n",result);

		result = getActiveFailContainerIds(client,&activeFailIds,&activeFailSize);
		printf("Active Fail Container Size:%d\n",activeFailSize);
		for (i = 0;i < activeFailSize;i++){
			printf("Active Fail Container Id:%d\n",activeFailIds[i]);
		}
		free(activeFailIds);
		freeMemAllocatedResourcesArray(allocatedResourcesArray, allocatedResourceArraySize);
		printf("freeMemAllocated is OK\n");

		//9. finish
		printf("jobId:%s\n", jobId);
		result = finishJob(client, jobId, APPLICATION_SUCCEEDED);
		printf("The finishJob Result Code:%d\n", result);
		if (result != FUNCTION_SUCCEEDED) {
			const char* errorMessage = getErrorMessage();
			printf(" The finishJob, errorMessage:%s\n", errorMessage);
		}
		free(jobId);
		//10. free client
		deleteLibYarnClient(client);

		return 0;
}
