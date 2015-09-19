#include <unistd.h>
#include "libyarn/LibYarnClientC.h"
#include "libyarn/LibYarnConstants.h"

int main() {
	char *rmHost = "localhost";
	char *rmPort = "8032";
	char *schedHost = "localhost";
	char *schedPort = "8030";
	char *amHost = "localhost";
	int32_t amPort = 0;
	char *am_tracking_url = "url";

	int i;

	//0. new client
	LibYarnClient_t *client = newLibYarnClient(rmHost, rmPort, schedHost, schedPort,
			amHost, amPort, am_tracking_url);

	//1. createJob
	char *jobName = "libyarn";
	char *queue = "default";
	char* jobId = createJob(client, jobName, queue);
	printf("1. createJob, jobid:%s\n", jobId);

	//2. allocate
	char *resGroupId = "group_1";
	LibYarnResourceRequest_t resRequest;
	resRequest.priority = 1;
	resRequest.host = "*";
	resRequest.vCores = 1;
	resRequest.memory = 1024;
	resRequest.num_containers = 3;
	resRequest.relax_locality = 1;

	char *blackListAdditions[0];
	char *blackListRemovals[0];

	LibYarnResource_t *allocatedResourcesArray;
	int allocatedResourceArraySize;
	allocateResources(client, jobId, resGroupId, &resRequest, blackListAdditions,
			0, blackListRemovals, 0, &allocatedResourcesArray, &allocatedResourceArraySize);


	printf("2. allocateResources, allocatedResourceArraySize:%d\n", allocatedResourceArraySize);
	for (i = 0 ; i < allocatedResourceArraySize; i++) {
		puts("----------------------------");
		printf("allocatedResourcesArray[i].containerId:%d\n", allocatedResourcesArray[i].containerId);
		printf("allocatedResourcesArray[i].host:%s\n", allocatedResourcesArray[i].host);
		printf("allocatedResourcesArray[i].port:%d\n", allocatedResourcesArray[i].port);
		printf("allocatedResourcesArray[i].nodeHttpAddress:%s\n", allocatedResourcesArray[i].nodeHttpAddress);

		printf("allocatedResourcesArray[i].vCores:%d\n", allocatedResourcesArray[i].vCores);
		printf("allocatedResourcesArray[i].memory:%d\n", allocatedResourcesArray[i].memory);
	}

	freeMemAllocatedResourcesArray(allocatedResourcesArray, allocatedResourceArraySize);

	//3. active
	activeResources(client, jobId, resGroupId);

	sleep(1);

	//4. release
	releaseResources(client, jobId, resGroupId);


	//5. getQueueInfo
	LibYarnQueueInfo_t *queueInfo = NULL;
	getQueueInfo(client, queue, true, true, true, &queueInfo);
	printf(
			"QueueInfo: queueInfo->queueName:%s, queueInfo->capacity:%f, queueInfo->maximumCapacity:%f, queueInfo->currentCapacity:%f, queueInfo->state:%d\n",
			queueInfo->queueName, queueInfo->capacity, queueInfo->maximumCapacity,
			queueInfo->currentCapacity, queueInfo->state);
	puts("---------chilldQueue:");
	for (i = 0; i < queueInfo->childQueueNameArraySize; i++) {
		printf("QueueInfo: queueInfo->childQueueNameArray[%d]:%s\n", i, queueInfo->childQueueNameArray[i]);
	}
	freeMemQueueInfo(queueInfo);


	//6. getCluster
	LibYarnNodeReport_t *nodeReportArray;
	int nodeReportArraySize;
	getClusterNodes(client, NODE_STATE_RUNNING, &nodeReportArray, &nodeReportArraySize);

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

	//7. finish
	printf("jobId:%s\n", jobId);
	finishJob(client, jobId, APPLICATION_SUCCEEDED);


	//8. free client
	deleteLibYarnClient(client);

	//9. free others
	free(jobId);

	return 0;
}
