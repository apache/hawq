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
		struct timeval tpstart,tpend;
		float allocateTime = 0;
		float activeTime = 0;
		float releaseTime = 0;
		float timeuse;
		char *rmHost = "localhost";
		char *rmPort = "8032";
		char *schedHost = "localhost";
		char *schedPort = "8030";
		char *amHost = "localhost";
		int32_t amPort = 10;
		char *am_tracking_url = "url";
		int heartbeatInterval = 1000;

		//0. new client
		LibYarnClient_t *client = NULL;
		int result = newLibYarnClient(rmHost, rmPort, schedHost, schedPort,
						amHost, amPort, am_tracking_url,&client,heartbeatInterval);

		//1. createJob
		char *jobName = "libyarn";
		char *queue = "default";
		char *jobId = NULL;
		result = createJob(client, jobName, queue,&jobId);
		if (result != FUNCTION_SUCCEEDED){
				const char* errorMessage = getErrorMessage();
				printf("1. createJob, errorMessage:%s\n",errorMessage);
		}

		int i,j;
		for (j = 0;j < 10;j++){
				LibYarnResourceRequest_t resRequest;
				resRequest.priority = 1;
				resRequest.host = "*";
				resRequest.vCores = 1;
				resRequest.memory = 1024;
				resRequest.num_containers = 2;
				resRequest.relax_locality = 1;

				char *blackListAdditions[0];
				char *blackListRemovals[0];

				//1. allocate resource
				LibYarnResource_t *allocatedResourcesArray;
				int allocatedResourceArraySize;
				gettimeofday(&tpstart,NULL);
				result = allocateResources(client, jobId, &resRequest, blackListAdditions,
								0, blackListRemovals, 0, &allocatedResourcesArray, &allocatedResourceArraySize,5);
				gettimeofday(&tpend,NULL);
			    timeuse=1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec;
			    allocateTime += timeuse;

				if (result != FUNCTION_SUCCEEDED){
						const char* errorMessage = getErrorMessage();
						printf("2. allocateResources, errorMessage:%s\n",errorMessage);
				}

				int32_t activeContainerIds[allocatedResourceArraySize];
				int32_t releaseContainerIds[allocatedResourceArraySize];
				int32_t statusContainerIds[allocatedResourceArraySize];
				for (i = 0 ; i < allocatedResourceArraySize; i++) {
						activeContainerIds[i] = allocatedResourcesArray[i].containerId;
						releaseContainerIds[i] = allocatedResourcesArray[i].containerId;
						statusContainerIds[i] = allocatedResourcesArray[i].containerId;
				}

				//3. active
				gettimeofday(&tpstart,NULL);
				result = activeResources(client, jobId, activeContainerIds,allocatedResourceArraySize);
				gettimeofday(&tpend,NULL);
				timeuse=1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec;
				activeTime += timeuse;
				if (result != FUNCTION_SUCCEEDED){
						const char* errorMessage = getErrorMessage();
						printf("2. activeResources, errorMessage:%s\n",errorMessage);
				}

				int *activeFailIds;
				int activeFailSize;
				result = getActiveFailContainerIds(client,&activeFailIds,&activeFailSize);
				for (i = 0;i < activeFailSize;i++){
						printf("Active Fail Container Id:%d\n",activeFailIds[i]);
				}
				//sleep(160);

				//4. getContainerReport
				LibYarnContainerReport_t *containerReportArray;
				int containerReportArraySize;
				result = getContainerReports(client, jobId, &containerReportArray,
						&containerReportArraySize);
				if (result != FUNCTION_SUCCEEDED) {
					const char* errorMessage = getErrorMessage();
					printf(" The getContainerReports, errorMessage:%s\n", errorMessage);
				}

				freeContainerReportArray(containerReportArray, containerReportArraySize);

				//4. getContainerReport
				LibYarnContainerStatus_t *containerStatusArray;
				int containerStatusArraySize;
				result = getContainerStatuses(client, jobId, statusContainerIds,
						allocatedResourceArraySize, &containerStatusArray,
						&containerStatusArraySize);
				if (result != FUNCTION_SUCCEEDED) {
					const char* errorMessage = getErrorMessage();
					printf(" The getContainerStatus, errorMessage:%s\n", errorMessage);
				}
				freeContainerStatusArray(containerStatusArray,containerStatusArraySize);

				//5. release
				gettimeofday(&tpstart,NULL);
				result = releaseResources(client, jobId, releaseContainerIds,allocatedResourceArraySize);
				gettimeofday(&tpend,NULL);
				timeuse=1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec;
				releaseTime += timeuse;

				result = getActiveFailContainerIds(client,&activeFailIds,&activeFailSize);
				for (i = 0;i < activeFailSize;i++){
						printf("Active Fail Container Id:%d\n",activeFailIds[i]);
				}
				free(activeFailIds);
				freeMemAllocatedResourcesArray(allocatedResourcesArray, allocatedResourceArraySize);

				//6. getQueueInfo
				LibYarnQueueInfo_t *queueInfo = NULL;
				result = getQueueInfo(client, queue, true, true, true, &queueInfo);
				if (result != FUNCTION_SUCCEEDED){
						const char* errorMessage = getErrorMessage();
						printf(" The getQueueInfo, errorMessage:%s\n",errorMessage);
				}

				freeMemQueueInfo(queueInfo);

				//7. getCluster
				LibYarnNodeReport_t *nodeReportArray;
				int nodeReportArraySize;
				result = getClusterNodes(client, NODE_STATE_RUNNING, &nodeReportArray, &nodeReportArraySize);
				if (result != FUNCTION_SUCCEEDED){
						const char* errorMessage = getErrorMessage();
						printf(" The getClusterNodes, errorMessage:%s\n",errorMessage);
				}

				freeMemNodeReportArray(nodeReportArray, nodeReportArraySize);

				//8. getApplicationReport
				LibYarnApplicationReport_t *applicationReport = NULL;
				result = getApplicationReport(client, jobId, &applicationReport);
				if (result != FUNCTION_SUCCEEDED) {
					const char* errorMessage = getErrorMessage();
					printf(" The getApplicationReport, errorMessage:%s\n",
							errorMessage);
				}

				freeApplicationReport(applicationReport);
		}
		printf("allocateTime:%f s\n",allocateTime/1000000);
		printf("activeTime:%f s\n",activeTime/1000000);
		printf("releaseTime:%f s\n",releaseTime/1000000);
		//9. finish
		result = finishJob(client, jobId, APPLICATION_SUCCEEDED);
		if (result != FUNCTION_SUCCEEDED) {
			const char* errorMessage = getErrorMessage();
			printf(" The finishJob, errorMessage:%s\n", errorMessage);
		}
		free(jobId);
		//10. free client
		deleteLibYarnClient(client);

		return 0;
}
