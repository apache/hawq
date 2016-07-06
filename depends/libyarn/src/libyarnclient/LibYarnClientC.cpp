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
#include <set>

#include "LibYarnClientC.h"
#include "LibYarnClient.h"
#include "records/ResourceRequest.h"
#include "records/Priority.h"

using std::string; using std::list;
using namespace libyarn;

extern "C" {

	const char* errorMessage;

	struct LibYarnClient_wrapper {
		public:
			LibYarnClient_wrapper(string &rmUser, string &rmHost, string &rmPort, string &schedHost,
							string &schedPort, string &amHost, int32_t amPort,
							string &am_tracking_url,int heartbeatInterval) {
				client = new LibYarnClient(rmUser, rmHost, rmPort, schedHost, schedPort, amHost,
								amPort, am_tracking_url,heartbeatInterval);
			}

			LibYarnClient_wrapper(LibYarnClient *libyarnClient){
				client = libyarnClient;
			}

			~LibYarnClient_wrapper() {
				delete client;
			}

			bool isJobHealthy() {
				return client->isJobHealthy();
			}

			const char* getErrorMessage() {
				return (client->getErrorMessage()).c_str();
			}

			int createJob(string &jobName, string &queue,string &jobId) {
				return client->createJob(jobName, queue, jobId);
			}

			int forceKillJob(string &jobId) {
				return client->forceKillJob(jobId);
			}

			int addContainerRequests(string &jobId,
									Resource &capability,
									int num_containers,
									list<struct LibYarnNodeInfo> &preferred,
									int priority,
									bool relax_locality) {
				return client->addContainerRequests(jobId, capability, num_containers,
								preferred, priority, relax_locality);
			}

			int allocateResources(string &jobId,
								  list<string> &blackListAdditions,
								  list<string> &blackListRemovals,
								  list<Container> &allocatedContainers,
								  int32_t num_containers) {
				return client->allocateResources(jobId, blackListAdditions, blackListRemovals,
								allocatedContainers, num_containers);
			}

			int activeResources(string &jobId,int64_t activeContainerIds[],int activeContainerSize) {
				return client->activeResources(jobId, activeContainerIds,activeContainerSize);
			}

			int releaseResources(string &jobId, int64_t releaseContainerIds[],int releaseContainerSize) {
				return client->releaseResources(jobId, releaseContainerIds,releaseContainerSize);
			}

			int finishJob(string &jobId, FinalApplicationStatus finalStatus) {
				return client->finishJob(jobId, finalStatus);
			}

			int getActiveFailContainerIds(set<int64_t> &activeFailIds){
				return client->getActiveFailContainerIds(activeFailIds);
			}

			int getApplicationReport(string &jobId, ApplicationReport &applicationReport) {
				return client->getApplicationReport(jobId,applicationReport);
			}

			int getContainerReports(string &jobId,list<ContainerReport> &containerReports) {
				return client->getContainerReports(jobId, containerReports);
			}

			int getContainerStatuses(string &jobId, int64_t containerIds[],
							int containerSize, list<ContainerStatus> &containerStatues) {
				return client->getContainerStatuses(jobId, containerIds, containerSize,containerStatues);
			}

			int getQueueInfo(string &queue, bool includeApps,
							bool includeChildQueues, bool recursive,QueueInfo &queueInfo) {
				return client->getQueueInfo(queue, includeApps, includeChildQueues,recursive,queueInfo);
			}

			int getClusterNodes(list<NodeState> &states,list<NodeReport> &nodeReports) {
				return client->getClusterNodes(states,nodeReports);
			}

		private:
			LibYarnClient *client;
	};

	LibYarnClient_t* getLibYarnClientT(LibYarnClient *libyarnClient){
		LibYarnClient_t *client = new LibYarnClient_t(libyarnClient);
		return client;
	}

	bool isJobHealthy(LibYarnClient_t *client) {
		return client->isJobHealthy();
	}

	//return error message
	const char* getErrorMessage(){
		return errorMessage;
	}

	void setErrorMessage(const char* errorMsg){
		errorMessage = errorMsg;
	}

	int  newLibYarnClient(char* user, char *rmHost, char *rmPort,
					char *schedHost, char *schedPort, char *amHost,
					int32_t amPort, char *am_tracking_url,LibYarnClient_t **client,int heartbeatInterval) {

		if(user == NULL || rmHost== NULL || rmPort == NULL || schedHost == NULL || schedPort == NULL
			|| amHost ==NULL || am_tracking_url == NULL)
			return FUNCTION_FAILED;

		string userStr(user);
		string rmHostStr(rmHost);
		string rmPortStr(rmPort);
		string schedHostStr(schedHost);
		string schedPortStr(schedPort);
		string amHostStr(amHost);
		string amTrackingUrlStr(am_tracking_url);
		*client = new LibYarnClient_t(userStr, rmHostStr, rmPortStr, schedHostStr, schedPortStr,
						amHostStr, amPort, amTrackingUrlStr,heartbeatInterval);
		return FUNCTION_SUCCEEDED;
	}

	void deleteLibYarnClient(LibYarnClient_t *client) {
		delete client;
	}

	int createJob(LibYarnClient_t *client, char *jobName, char *queue,char **jobId) {
		string jobNameStr(jobName);
		string queueStr(queue);
		string jobIdStr;
		if (*jobId == NULL) {
			jobIdStr = "";
		} else{
			jobIdStr = *jobId;
		}
		int result = client->createJob(jobNameStr,queueStr,jobIdStr);
		if (result == FUNCTION_SUCCEEDED){
			*jobId = strdup(jobIdStr.c_str());
			return FUNCTION_SUCCEEDED;
		}else{
			*jobId = strdup(jobIdStr.c_str());
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int forceKillJob(LibYarnClient_t *client, char* jobId) {
		string jobIdStr(jobId);
		int result = client->forceKillJob(jobIdStr);
		if (result == FUNCTION_SUCCEEDED) {
			return FUNCTION_SUCCEEDED;
		} else {
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int allocateResources(LibYarnClient_t *client, char *jobId,
						int32_t priority, int32_t vCores, int32_t memory, int32_t num_containers,
						char *blackListAdditions[], int blacklistAddsSize,
						char *blackListRemovals[], int blackListRemovalsSize,
						LibYarnNodeInfo_t preferredHosts[], int preferredHostSize,
						LibYarnResource_t **allocatedResourcesArray, int *allocatedResourceArraySize) {
		int i = 0;
		int totalPreferred = 0;
		string jobIdStr(jobId);
		list<string> blackListAdditionsList;
		list<string> blackListRemovalsList;
		list<Container> allocatedPreferredList;
		list<Container> allocatedAnyList;
		Resource capability;
		LibYarnResource_t *allAllocatedArray = NULL;
		LibYarnResource_t *preferredAllocatedArray = NULL;
		int preferredAllocatedSize = 0;
		int anyAllocatedSize = 0;

		capability.setVirtualCores(vCores);
		capability.setMemory(memory);

		list<LibYarnNodeInfo> preferredHostsList;
		if (preferredHosts != NULL && preferredHostSize > 0) {
			for (i = 0; i < preferredHostSize; i++) {
				LibYarnNodeInfo *info = new LibYarnNodeInfo(string(preferredHosts[i].hostname),
															string(preferredHosts[i].rackname),
															preferredHosts[i].num_containers);
				preferredHostsList.push_back(*info);
				totalPreferred += preferredHosts[i].num_containers;
			}
		}

		/* generate some resource requests and store them into a list. */
		int result = client->addContainerRequests(jobIdStr, capability,
												  (preferredHosts != NULL && preferredHostSize > 0)?totalPreferred:num_containers,
												  preferredHostsList, priority,
												  (preferredHosts != NULL && preferredHostSize > 0)?false:true);
		if (result != FUNCTION_SUCCEEDED)
			goto exit_err;

		for (i = 0; i < blacklistAddsSize; i++) {
			string tmp(blackListAdditions[i]);
			blackListAdditionsList.push_back(tmp);
		}

		for (i = 0; i < blackListRemovalsSize; i++) {
			string tmp(blackListRemovals[i]);
			blackListRemovalsList.push_back(tmp);
		}

		/*
		 * If the total requested container number is larger than the container number of preferred hosts,
		 * it'll request containers twice from YARN:
		 * The first is for two cases:
		 * 1. The requests don't have any preferred host request(all the requests is for ANY),
		 * 2. The requests are all for preferred hosts.
		 */
		result = client->allocateResources(jobIdStr, blackListAdditionsList, blackListRemovalsList,
				allocatedPreferredList, (preferredHosts != NULL && preferredHostSize)> 0?totalPreferred:num_containers);
		if (result != FUNCTION_SUCCEEDED)
			goto exit_err;

		preferredAllocatedSize = allocatedPreferredList.size();
		preferredAllocatedArray = (LibYarnResource_t *)malloc(sizeof(LibYarnResource_t) * preferredAllocatedSize);
		if(preferredAllocatedArray == NULL) {
			setErrorMessage("LibYarnClientC::fail to allocate memory for resource array");
			goto exit_err;
		}

		i = 0;
		for (list<Container>::iterator it = allocatedPreferredList.begin(); it != allocatedPreferredList.end(); it++) {
			preferredAllocatedArray[i].containerId = it->getId().getId();
			preferredAllocatedArray[i].host = strdup(it->getNodeId().getHost().c_str());
			preferredAllocatedArray[i].port = it->getNodeId().getPort();
			preferredAllocatedArray[i].vCores = it->getResource().getVirtualCores();
			preferredAllocatedArray[i].memory = it->getResource().getMemory();
			preferredAllocatedArray[i].nodeHttpAddress = strdup(it->getNodeHttpAddress().c_str());
			i++;
		}

		if (preferredHostsList.size() > 0 && num_containers-totalPreferred > 0) {
			/*
			 * The second is for ANY, except preferred host.
			 * add remain requests for ANY, relax_locality is true.
			 * */
			result = client->addContainerRequests(jobIdStr, capability, num_containers-totalPreferred,
												  preferredHostsList, priority, true);
			if (result != FUNCTION_SUCCEEDED) {
				goto exit_part;
			}

			result = client->allocateResources(jobIdStr, blackListAdditionsList, blackListRemovalsList,
									allocatedAnyList, num_containers-totalPreferred);
			if (result != FUNCTION_SUCCEEDED)
				goto exit_part;

			anyAllocatedSize = allocatedAnyList.size();
			allAllocatedArray = (LibYarnResource_t *) realloc(preferredAllocatedArray,
														sizeof(LibYarnResource_t) * (preferredAllocatedSize + anyAllocatedSize));
			if(allAllocatedArray == NULL) {
				setErrorMessage("LibYarnClientC::fail to re-allocate memory for resource array");
				goto exit_part;
			}
			i = 0;
			for (list<Container>::iterator it = allocatedAnyList.begin(); it != allocatedAnyList.end(); it++) {
				allAllocatedArray[preferredAllocatedSize+i].containerId = it->getId().getId();
				allAllocatedArray[preferredAllocatedSize+i].host = strdup(it->getNodeId().getHost().c_str());
				allAllocatedArray[preferredAllocatedSize+i].port = it->getNodeId().getPort();
				allAllocatedArray[preferredAllocatedSize+i].vCores = it->getResource().getVirtualCores();
				allAllocatedArray[preferredAllocatedSize+i].memory = it->getResource().getMemory();
				allAllocatedArray[preferredAllocatedSize+i].nodeHttpAddress = strdup(it->getNodeHttpAddress().c_str());
				i++;
			}

			*allocatedResourcesArray = allAllocatedArray;
			*allocatedResourceArraySize = preferredAllocatedSize + anyAllocatedSize;
		}
		else {
			*allocatedResourcesArray = preferredAllocatedArray;
			*allocatedResourceArraySize = preferredAllocatedSize;
		}

		preferredHostsList.clear();
		return FUNCTION_SUCCEEDED;

exit_part:
		/*
		 * The first allocation succeeds,
		 * return containers allocated at first time.
		 */
		*allocatedResourcesArray = preferredAllocatedArray;
		*allocatedResourceArraySize = preferredAllocatedSize;
		preferredHostsList.clear();
		return FUNCTION_SUCCEEDED;

exit_err:
		preferredHostsList.clear();
		setErrorMessage(client->getErrorMessage());
		return FUNCTION_FAILED;
	}

	int activeResources(LibYarnClient_t *client, char *jobId,int64_t activeContainerIds[],int activeContainerSize){
		string jobIdStr(jobId);
		int result = client->activeResources(jobIdStr,activeContainerIds,activeContainerSize);
		if (result == FUNCTION_SUCCEEDED){
			return FUNCTION_SUCCEEDED;
		}else{
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int releaseResources(LibYarnClient_t *client, char *jobId,int64_t releaseContainerIds[], int releaseContainerSize) {
		string jobIdStr(jobId);
		int result = client->releaseResources(jobIdStr, releaseContainerIds, releaseContainerSize);
		if (result == FUNCTION_SUCCEEDED) {
			return FUNCTION_SUCCEEDED;
		} else {
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int finishJob(LibYarnClient_t *client, char *jobId,FinalApplicationStatus_t finalStatus) {
		string jobIdStr(jobId);
		int result = client->finishJob(jobIdStr, (FinalApplicationStatus) finalStatus);
		if (result == FUNCTION_SUCCEEDED) {
			return FUNCTION_SUCCEEDED;
		} else {
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int getActiveFailContainerIds(LibYarnClient_t *client,int64_t *activeFailIds[],int *activeFailSize){
		set<int64_t> activeFails;
		int result = client->getActiveFailContainerIds(activeFails);
		if (result == FUNCTION_SUCCEEDED) {
			*activeFailSize = activeFails.size();
			*activeFailIds = (int64_t *)malloc(sizeof(int64_t)*(*activeFailSize));
			int i = 0;
			for (set<int64_t>::iterator it = activeFails.begin(); it != activeFails.end(); it++) {
					(*activeFailIds)[i] = (*it);
					i++;
			}
			return FUNCTION_SUCCEEDED;
		}else{
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int getApplicationReport(LibYarnClient_t *client,char *jobId,
							LibYarnApplicationReport_t **applicationReport) {
		string jobIdStr(jobId);
		ApplicationReport applicationReportCpp;
		int result = client->getApplicationReport(jobIdStr,applicationReportCpp);
		if (result == FUNCTION_SUCCEEDED){
			*applicationReport = (LibYarnApplicationReport_t *) malloc(sizeof (LibYarnApplicationReport_t));
			(*applicationReport)->appId = applicationReportCpp.getApplicationId().getId();
			(*applicationReport)->user = strdup(applicationReportCpp.getUser().c_str());
			(*applicationReport)->queue = strdup(applicationReportCpp.getQueue().c_str());
			(*applicationReport)->name = strdup(applicationReportCpp.getName().c_str());
			(*applicationReport)->host = strdup(applicationReportCpp.getHost().c_str());
			(*applicationReport)->port = applicationReportCpp.getRpcPort();
			(*applicationReport)->url = strdup(applicationReportCpp.getTrackingUrl().c_str());
			(*applicationReport)->status = applicationReportCpp.getYarnApplicationState();
			(*applicationReport)->diagnostics = strdup(applicationReportCpp.getDiagnostics().c_str());
			(*applicationReport)->startTime = applicationReportCpp.getStartTime();
			(*applicationReport)->progress = applicationReportCpp.getProgress();
			return FUNCTION_SUCCEEDED;
		} else{
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int getContainerReports(LibYarnClient_t *client, char *jobId,
			LibYarnContainerReport_t **containerReportArray,int *containerReportArraySize) {
		string jobIdStr(jobId);
		list < ContainerReport > containerReports;
		int result = client->getContainerReports(jobIdStr,containerReports);
		if (result == FUNCTION_SUCCEEDED){
			*containerReportArraySize = containerReports.size();
			*containerReportArray = (LibYarnContainerReport_t *)malloc(sizeof(LibYarnContainerReport_t) * containerReports.size());
			int i = 0;
			for (list<ContainerReport>::iterator it = containerReports.begin();
					it != containerReports.end(); it++){
				(*containerReportArray)[i].containerId = it->getId().getId();
				(*containerReportArray)[i].vCores = it->getResource().getVirtualCores();
				(*containerReportArray)[i].memory = it->getResource().getMemory();
				(*containerReportArray)[i].host = strdup(it->getNodeId().getHost().c_str());
				(*containerReportArray)[i].port = it->getNodeId().getPort();
				(*containerReportArray)[i].exitStatus = it->getContainerExitStatus();
				(*containerReportArray)[i].state = it->getContainerState();
				(*containerReportArray)[i].diagnostics = strdup(it->getDiagnostics().c_str());
				i++;
			}
			return FUNCTION_SUCCEEDED;
		}else{
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int getContainerStatuses(LibYarnClient_t *client,char *jobId,int64_t containerIds[],int containerSize,
					LibYarnContainerStatus_t **containerStatusesArray,int *containerStatusesArraySize){
		string jobIdStr(jobId);
		list < ContainerStatus > containerStatuses;
		int result = client->getContainerStatuses(jobIdStr,containerIds,containerSize,containerStatuses);
		if (result == FUNCTION_SUCCEEDED) {
			*containerStatusesArraySize = containerStatuses.size();
			*containerStatusesArray = (LibYarnContainerStatus_t *) malloc(sizeof(LibYarnContainerStatus_t) * containerStatuses.size());
			int i = 0;
			for (list<ContainerStatus>::iterator it = containerStatuses.begin();
					it != containerStatuses.end(); it++) {
				(*containerStatusesArray)[i].containerId = it->getContainerId().getId();
				(*containerStatusesArray)[i].exitStatus = it->getExitStatus();
				(*containerStatusesArray)[i].state = it->getContainerState();
				(*containerStatusesArray)[i].diagnostics =  strdup(it->getDiagnostics().c_str());
				i++;
			}
			return FUNCTION_SUCCEEDED;
		} else {
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}


	int getQueueInfo(LibYarnClient_t *client, char *queue, bool includeApps,
					bool includeChildQueues, bool recursive, LibYarnQueueInfo_t **queueInfo) {
		string queueStr(queue);
		QueueInfo queueInfoCpp;
		int result = client->getQueueInfo(queueStr, includeApps, includeChildQueues,recursive, queueInfoCpp);

		if (result == FUNCTION_SUCCEEDED) {
			*queueInfo = (LibYarnQueueInfo_t *) malloc(sizeof(LibYarnQueueInfo_t));
			(*queueInfo)->queueName = strdup(queueInfoCpp.getQueueName().c_str());
			(*queueInfo)->capacity = queueInfoCpp.getCapacity();
			(*queueInfo)->maximumCapacity = queueInfoCpp.getMaximumCapacity();
			(*queueInfo)->currentCapacity = queueInfoCpp.getCurrentCapacity();
			(*queueInfo)->state = queueInfoCpp.getQueueState();

			list<QueueInfo> childQueueInfos = queueInfoCpp.getChildQueues();
			(*queueInfo)->childQueueNameArraySize = childQueueInfos.size();
			(*queueInfo)->childQueueNameArray = (char**) malloc(sizeof(char *) * childQueueInfos.size());
			int i = 0;
			for (list<QueueInfo>::iterator it = childQueueInfos.begin();
					it != childQueueInfos.end(); it++) {
				(*queueInfo)->childQueueNameArray[i] = strdup(it->getQueueName().c_str());
				i++;
			}
			return FUNCTION_SUCCEEDED;
		} else {
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	int getClusterNodes(LibYarnClient_t *client, NodeState_t state,
					LibYarnNodeReport_t **nodeReportArray, int *nodeReportArraySize) {
		list<NodeState> states;
		states.push_back((NodeState) state);
		list<NodeReport> nodeReports;
		int result = client->getClusterNodes(states, nodeReports);
		if (result == FUNCTION_SUCCEEDED) {
			*nodeReportArraySize = nodeReports.size();
			*nodeReportArray = (LibYarnNodeReport_t *) malloc(sizeof(LibYarnNodeReport_t) * nodeReports.size());
			int i = 0;
			for (list<NodeReport>::iterator it = nodeReports.begin();
					it != nodeReports.end(); it++) {
				(*nodeReportArray)[i].host = strdup(it->getNodeId().getHost().c_str());
				(*nodeReportArray)[i].port = it->getNodeId().getPort();
				(*nodeReportArray)[i].httpAddress = strdup(it->getHttpAddress().c_str());
				(*nodeReportArray)[i].rackName = strdup(it->getRackName().c_str());
				(*nodeReportArray)[i].memoryUsed = it->getUsedResource().getMemory();
				(*nodeReportArray)[i].vcoresUsed = it->getUsedResource().getVirtualCores();
				(*nodeReportArray)[i].memoryCapability = it->getResourceCapability().getMemory();
				(*nodeReportArray)[i].vcoresCapability = it->getResourceCapability().getVirtualCores();
				(*nodeReportArray)[i].numContainers = it->getNumContainers();
				(*nodeReportArray)[i].nodeState = it->getNodeState();
				(*nodeReportArray)[i].healthReport = strdup(it->getHealthReport().c_str());
				(*nodeReportArray)[i].lastHealthReportTime = it->getLastHealthReportTime();
				i++;
			}
			return FUNCTION_SUCCEEDED;
		} else {
			setErrorMessage(client->getErrorMessage());
			return FUNCTION_FAILED;
		}
	}

	void freeMemAllocatedResourcesArray(LibYarnResource_t *allocatedResourcesArray,
				int allocatedResourceArraySize) {
		for (int i = 0; i < allocatedResourceArraySize; i++) {
			free(allocatedResourcesArray[i].host);
			free(allocatedResourcesArray[i].nodeHttpAddress);
		}
		free(allocatedResourcesArray);
	}

	void freeApplicationReport(LibYarnApplicationReport_t *applicationReport){
		free(applicationReport->user);
		free(applicationReport->queue);
		free(applicationReport->name);
		free(applicationReport->host);
		free(applicationReport->url);
		free(applicationReport->diagnostics);
		free(applicationReport);
	}

	void freeContainerReportArray(LibYarnContainerReport_t *containerReportArray,int containerReportArraySize){
		for (int i = 0; i < containerReportArraySize; i++) {
			free(containerReportArray[i].host);
			free(containerReportArray[i].diagnostics);
		}
		free(containerReportArray);
	}

	void freeContainerStatusArray(LibYarnContainerStatus_t *containerStatusesArray,int containerStatusesArraySize){
		for (int i = 0; i < containerStatusesArraySize; i++) {
			free(containerStatusesArray[i].diagnostics);
		}
		free(containerStatusesArray);
	}

	void freeMemQueueInfo(LibYarnQueueInfo_t *queueInfo) {
		free(queueInfo->queueName);
		for (int i = 0; i < queueInfo->childQueueNameArraySize; i++) {
			free(queueInfo->childQueueNameArray[i]);
		}
		free(queueInfo->childQueueNameArray);
		free(queueInfo);
	}

	void freeMemNodeReportArray(LibYarnNodeReport_t *nodeReportArray, int nodeReportArraySize) {
		for (int i = 0; i < nodeReportArraySize; i++) {
			free(nodeReportArray[i].host);
			free(nodeReportArray[i].httpAddress);
			free(nodeReportArray[i].rackName);
			free(nodeReportArray[i].healthReport);
		}
		free(nodeReportArray);
	}
}
