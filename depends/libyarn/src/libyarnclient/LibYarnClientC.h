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

#ifndef LIBYARNCLIENTC_H_
#define LIBYARNCLIENTC_H_

#include <stdlib.h>
#include <stdint.h> /* for uint64_t, etc. */
#include <stdbool.h>

#include "LibYarnConstants.h"

#include "records/YarnApplicationState.h"
#include "records/QueueState.h"
#include "records/NodeState.h"
#include "records/ContainerExitStatus.h"
#include "records/ContainerState.h"

#ifdef __cplusplus
extern "C" {
#endif


struct LibYarnClient_wrapper;
typedef struct LibYarnClient_wrapper LibYarnClient_t;

typedef struct LibYarnResourceRequest_t {
	int32_t priority;
	char *host;
	int32_t vCores;
	int32_t memory;
	int32_t num_containers;
	bool relax_locality;
} LibYarnResourceRequest_t;

typedef struct LibYarnResource_t {
	int64_t containerId;
	char *host;
	int32_t port;
	char *nodeHttpAddress;
	int32_t vCores;
	int32_t memory;
} LibYarnResource_t;

typedef struct LibYarnApplicationReport_t {
	int appId;
	char *user;
	char *queue;
	char *name;
	char *host;
	int32_t port;
	char *url;
	enum YarnApplicationState status;
	char *diagnostics;
	int64_t startTime;
	float progress;
}LibYarnApplicationReport_t;

typedef struct LibYarnContainerReport_t {
	int64_t containerId;
	int32_t vCores;
	int32_t memory;
	char *host;
	int32_t port;
	int64_t startTime;
	int64_t finishTime;
	enum ContainerExitStatus exitStatus;
	enum ContainerState state;
	char *diagnostics;
}LibYarnContainerReport_t;

typedef struct LibYarnContainerStatus_t {
	int64_t containerId;
	enum ContainerState state;
	int32_t exitStatus;
	char *diagnostics;
}LibYarnContainerStatus_t;

typedef struct LibYarnQueueInfo_t {
	char *queueName;
	float capacity;
	float maximumCapacity;
	float currentCapacity;
	enum QueueState state;
	char **childQueueNameArray;
	int childQueueNameArraySize;
} LibYarnQueueInfo_t;

typedef struct LibYarnNodeReport_t {
	char *host;
	int32_t port;
	char *httpAddress;
	char *rackName;
	int32_t memoryUsed;
	int32_t vcoresUsed;
	int32_t memoryCapability;
	int32_t vcoresCapability;
	int32_t numContainers;
	enum NodeState nodeState;
	char *healthReport;
	int64_t lastHealthReportTime;
} LibYarnNodeReport_t;

typedef struct LibYarnNodeInfo_t {
	char *hostname;
	char *rackname;
	int32_t num_containers;  // number of containers on this host/rack
} LibYarnNodeInfo_t;

enum NodeState_t {
  NODE_STATE_NEW = 1,
  NODE_STATE_RUNNING = 2,
  NODE_STATE_UNHEALTHY = 3,
  NODE_STATE_DECOMMISSIONED = 4,
  NODE_STATE_LOST = 5,
  NODE_STATE_REBOOTED = 6
};

enum FinalApplicationStatus_t {
  APPLICATION_UNDEFINED = 0,
  APPLICATION_SUCCEEDED = 1,
  APPLICATION_FAILED = 2,
  APPLICATION_KILLED = 3
};

enum FunctionResult_t {
  FUNCTION_SUCCEEDED = 0,
  FUNCTION_FAILED = 1
};

bool isJobHealthy(LibYarnClient_t *client);

const char* getErrorMessage();

void setErrorMessage(const char* errorMsg);

int newLibYarnClient(char* user, char *rmHost, char *rmPort,
				char *schedHost, char *schedPort, char *amHost,
				int32_t amPort, char *am_tracking_url,LibYarnClient_t **client,int heartbeatInterval);

void deleteLibYarnClient(LibYarnClient_t *client);

int createJob(LibYarnClient_t *client, char *jobName, char *queue,char **jobId);

int forceKillJob(LibYarnClient_t *client, char *jobId);

int addContainerRequest(LibYarnNodeInfo_t preferredHosts[], int preferredHostsSize,
						int32_t priority, bool relax_locality);

int allocateResources(LibYarnClient_t *client, char *jobId,
				int32_t priority, int32_t vCores, int32_t memory, int32_t num_containers,
				char *blackListAdditions[], int blacklistAddsSize,
				char *blackListRemovals[], int blackListRemovalsSize,
				LibYarnNodeInfo_t preferredHost[], int preferredHostSize,
				LibYarnResource_t **allocatedResourcesArray, int *allocatedResourceArraySize);

int activeResources(LibYarnClient_t *client, char *jobId,int64_t activeContainerIds[],int activeContainerSize);

int releaseResources(LibYarnClient_t *client, char *jobId, int64_t releaseContainerIds[],int releaseContainerSize);

int finishJob(LibYarnClient_t *client, char *jobId, enum FinalApplicationStatus_t finalStatus);

int getActiveFailContainerIds(LibYarnClient_t *client,int64_t *activeFailIds[],int *activeFailSize);

int getApplicationReport(LibYarnClient_t *client,char *jobId,LibYarnApplicationReport_t **applicationReport);

int getContainerReports(LibYarnClient_t *client, char *jobId,
				LibYarnContainerReport_t **containerReportArray,int *containerReportArraySize);

int getContainerStatuses(LibYarnClient_t *client,char *jobId,int64_t containerIds[],int containerSize,
				LibYarnContainerStatus_t **containerStatusesArray,int *containerStatusesArraySize);

int getQueueInfo(LibYarnClient_t *client, char *queue, bool includeApps,
				bool includeChildQueues, bool recursive, LibYarnQueueInfo_t **queueInfo);

int getClusterNodes(LibYarnClient_t *client, enum NodeState_t state,
				LibYarnNodeReport_t **nodeReportArray, int *nodeReportArraySize);

void freeMemAllocatedResourcesArray(LibYarnResource_t *allocatedResourcesArray,int allocatedResourceArraySize);

void freeApplicationReport(LibYarnApplicationReport_t *applicationReport);

void freeContainerReportArray(LibYarnContainerReport_t *containerReportArray,int containerReportArraySize);

void freeContainerStatusArray(LibYarnContainerStatus_t *containerStatusesArray,int containerStatusesArraySize);

void freeMemQueueInfo(LibYarnQueueInfo_t *queueInfo);

void freeMemNodeReportArray(LibYarnNodeReport_t *nodeReportArray, int nodeReportArraySize);

#ifdef __cplusplus
}
#endif

#endif /* LIBYARNCLIENTC_H_ */
