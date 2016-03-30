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

#include <iostream>
#include <list>

#include <unistd.h>

#include "libyarn/LibYarnClient.h"
#include "libyarn/records/FinalApplicationStatus.h"

using std::string;
using std::cout;
using std::endl;
using std::list;
using namespace libyarn;

int main(void)
{

	string rmHost("localhost");
	string rmPort("8032");
	string schedHost("localhost");
	string schedPort("8030");
	string amHost("localhost");
	int32_t amPort = 0;


	//cluster node1
/*
		string rmHost("10.37.7.101");
		string rmPort("8032");
		string schedHost("10.37.7.101");
		string schedPort("8030");
		string amHost("10.34.0.134");
		int32_t amPort = 0;
*/

	string am_tracking_url("url");
	LibYarnClient client(rmHost, rmPort, schedHost, schedPort, amHost, amPort, am_tracking_url);

	//1. createJob
	string jobName("libyarn");
	string queue("default");
	string jobId = client.createJob(jobName, queue);
	cout<<"jobId:"<<jobId<<endl;

	//2. allocate
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


	string resGroupId("group_1");
	list<string> blackListAdditions;
	list<string> blackListRemovals;

	list<Container> allocatedResources = client.allocateResources(jobId,
				resGroupId, resRequest, blackListAdditions, blackListRemovals);

	for (list<Container>::iterator it = allocatedResources.begin();
			it != allocatedResources.end(); it++) {
		cout << "allocate: host:" << (*it).getNodeId().getHost() << ", port:"
				<< (*it).getNodeId().getPort() << ", cid:" << (*it).getId().getId()
				<< ", vcores:" << (*it).getResource().getVirtualCores() << ", mem:"
				<< (*it).getResource().getMemory() << endl;
	}

	//3. active
	client.activeResources(jobId, resGroupId);

	sleep(1);

	//4. release
	client.releaseResources(jobId, resGroupId);

	sleep(2);

	//5. getQueueInfo
	QueueInfo queueInfo = client.getQueueInfo(queue, true, true, true);
	cout << "queueName:" << queueInfo.getQueueName() << ", capacity:"
			<< queueInfo.getCapacity() << ", maximumCapacity:"
			<< queueInfo.getMaximumCapacity() << ", currentCapacity:"
			<< queueInfo.getCurrentCapacity() << ", state:"
			<< queueInfo.getQueueState() << endl;

	//6. getCluster
	list<NodeState> nodeStates;
	nodeStates.push_back(NodeState::NS_RUNNING);
	list<NodeReport> nodeReports = client.getClusterNodes(nodeStates);
	for (list<NodeReport>::iterator it = nodeReports.begin(); it != nodeReports.end(); it++) {
		cout << "host:" << it->getNodeId().getHost() << ", port:"
				<< it->getNodeId().getPort() << ", httpAddress:" << it->getHttpAddress()
				<< ", rackName:" << it->getRackName() << ", used:[vcore:"
				<< it->getUsedResource().getVirtualCores() << ", mem:"
				<< it->getUsedResource().getMemory() << "], capability:[vcore:"
				<< it->getResourceCapability().getVirtualCores() << ", mem:"
				<< it->getResourceCapability().getMemory() << "], numContainers:"
				<< it->getNumContainers() << ", node_state:" << it->getNodeState()
				<< ", health_report:" << it->getHealthReport()
				<< ", last_health_report_time:" << it->getLastHealthReportTime()
				<< endl;
	}

	//7. finish
	cout << "to finish: jobId:" << jobId << endl;
	client.finishJob(jobId, FinalApplicationStatus::APP_SUCCEEDED);

	sleep(1);


	return 0;
}
