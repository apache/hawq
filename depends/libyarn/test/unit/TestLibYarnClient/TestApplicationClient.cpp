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
#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "libyarnclient/ApplicationClient.h"
#include "MockApplicationClientProtocol.h"

using std::string;
using std::list;
using namespace libyarn;
using namespace testing;
using namespace Mock;

class TestApplicationClient: public ::testing::Test {
public:
	TestApplicationClient(){
		string user("postgres");
		string rmHost("localhost");
		string rmPort("8032");
		string tokenService = "";
		Yarn::Config config;
		Yarn::Internal::SessionConfig sessionConfig(config);
		MockApplicationClientProtocol *protocol = new MockApplicationClientProtocol(user,rmHost,rmPort,tokenService, sessionConfig);

		ApplicationId appId;
		appId.setId(100);
		appId.setClusterTimestamp(1454307175682);
		GetNewApplicationResponse getNewApplicationResponse;
		getNewApplicationResponse.setApplicationId(appId);
		EXPECT_CALL((*protocol),getNewApplication(_)).Times(AnyNumber()).WillOnce(Return(getNewApplicationResponse));
		EXPECT_CALL((*protocol),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());

		ApplicationReport appReport;
		appReport.setApplicationId(appId);
		appReport.setUser(user);
		string queue("default");
		string appName("hawq");
		string hostname("master");
		appReport.setQueue(queue);
		appReport.setName(appName);
		appReport.setHost(hostname);
		appReport.setRpcPort(8090);
		appReport.setProgress(0.5);
		GetApplicationReportResponse appReportResponse;
		appReportResponse.setApplicationReport(appReport);
		EXPECT_CALL((*protocol),getApplicationReport(_)).Times(AnyNumber()).WillOnce(Return(appReportResponse));

		ContainerId containerId;
		containerId.setId(501);
		containerId.setApplicationId(appId);
		Resource resource;
		resource.setMemory(1024);
		resource.setVirtualCores(1);
		Priority priority;
		priority.setPriority(1);
		ContainerReport report;
		report.setId(containerId);
		report.setResource(resource);
		report.setPriority(priority);
		list<ContainerReport> reportList;
		reportList.push_back(report);
		GetContainersResponse getContainersResponse;
		getContainersResponse.setContainersReportList(reportList);
		EXPECT_CALL((*protocol),getContainers(_)).Times(AnyNumber()).WillOnce(Return(getContainersResponse));

		NodeId nodeId;
		string nodeHost("node1");
		nodeId.setHost(nodeHost);
		nodeId.setPort(9983);
		NodeReport nodeReport;
		nodeReport.setNodeId(nodeId);
		string rackName("default-rack");
		nodeReport.setRackName(rackName);
		nodeReport.setNumContainers(8);
		Resource nodeResource;
		nodeResource.setMemory(2048*8);
		nodeResource.setVirtualCores(8);
		nodeReport.setResourceCapablity(nodeResource);
		nodeReport.setNodeState(NodeState::NS_RUNNING);
		list<NodeReport> nodeReportList;
		nodeReportList.push_back(nodeReport);
		GetClusterNodesResponse getClusterNodesResponse;
		getClusterNodesResponse.setNodeReports(nodeReportList);
		EXPECT_CALL((*protocol),getClusterNodes(_)).Times(AnyNumber()).WillOnce(Return(getClusterNodesResponse));

		QueueInfo queueInfo;
		queueInfo.setQueueName(queue);
		queueInfo.setCapacity(0.67);
		queueInfo.setMaximumCapacity(0.95);
		queueInfo.setCurrentCapacity(0.5);
		queueInfo.setQueueState(QueueState::Q_RUNNING);
		QueueInfo childQueue;
		string childQueueName("hawq-queue");
		childQueue.setQueueName(childQueueName);
		childQueue.setCapacity(0.33);
		childQueue.setMaximumCapacity(0.5);
		childQueue.setCurrentCapacity(0.25);
		list<QueueInfo> childQueueList;
		childQueueList.push_back(childQueue);
		queueInfo.setChildQueues(childQueueList);
		list<ApplicationReport> appReportList;
		appReportList.push_back(appReport);
		queueInfo.setApplicationReports(appReportList);
		GetQueueInfoResponse getQueueInfoResponse;
		getQueueInfoResponse.setQueueInfo(queueInfo);
		EXPECT_CALL((*protocol),getQueueInfo(_)).Times(AnyNumber()).WillOnce(Return(getQueueInfoResponse));

		KillApplicationResponseProto killApplicationResponseProto;
		EXPECT_CALL((*protocol),forceKillApplication(_)).Times(AnyNumber()).WillOnce(Return(KillApplicationResponse(killApplicationResponseProto)));

		YarnClusterMetrics metrics;
		metrics.setNumNodeManagers(10);
		GetClusterMetricsResponse clusterMetricsResponse;
		clusterMetricsResponse.setClusterMetrics(metrics);
		EXPECT_CALL((*protocol),getClusterMetrics(_)).Times(AnyNumber()).WillOnce(Return(clusterMetricsResponse));

		GetApplicationsResponse applicationsResponse;
		applicationsResponse.setApplicationList(appReportList);
		EXPECT_CALL((*protocol),getApplications(_)).Times(AnyNumber()).WillOnce(Return(applicationsResponse));

		QueueUserACLInfo aclInfo;
		aclInfo.setQueueName(queue);
		list<QueueACL> queueACLList;
		QueueACL acl1 = QueueACL::QACL_ADMINISTER_QUEUE;
		QueueACL acl2 = QueueACL::QACL_SUBMIT_APPLICATIONS;
		queueACLList.push_back(acl1);
		queueACLList.push_back(acl2);
		aclInfo.setUserAcls(queueACLList);
		list<QueueUserACLInfo> aclInfoList;
		aclInfoList.push_back(aclInfo);
		GetQueueUserAclsInfoResponse queueUserAclsInfoResponse;
		queueUserAclsInfoResponse.setUserAclsInfoList(aclInfoList);
		EXPECT_CALL((*protocol),getQueueAclsInfo(_)).Times(AnyNumber()).WillOnce(Return(queueUserAclsInfoResponse));

		client = new ApplicationClient(protocol);
	}

	~TestApplicationClient(){
		delete client;
	}

protected:
	ApplicationClient *client;
};

TEST_F(TestApplicationClient, TestGetNewApplication){
	ApplicationId response = client->getNewApplication();
	EXPECT_EQ(response.getId(), 100);
	EXPECT_EQ(response.getClusterTimestamp(), 1454307175682);
}

TEST_F(TestApplicationClient,TestSubmitApplication){
	ApplicationSubmissionContext appContext;
	client->submitApplication(appContext);
}

TEST_F(TestApplicationClient,TestGetApplicationReport){
	ApplicationId appId;
	ApplicationReport report = client->getApplicationReport(appId);
	EXPECT_EQ(report.getUser(), "postgres");
	EXPECT_EQ(report.getQueue(), "default");
	EXPECT_EQ(report.getName(), "hawq");
	EXPECT_EQ(report.getHost(), "master");
	EXPECT_EQ(report.getRpcPort(), 8090);
	EXPECT_FLOAT_EQ(report.getProgress(), 0.5);
}

TEST_F(TestApplicationClient,TestGetContainers){
	ApplicationAttemptId appAttempId;
	list<ContainerReport> reports = client->getContainers(appAttempId);
	EXPECT_EQ(reports.size(), 1);
	list<ContainerReport>::iterator it = reports.begin();
	EXPECT_EQ(it->getId().getId(), 501);
	EXPECT_EQ(it->getPriority().getPriority(), 1);
	EXPECT_EQ(it->getResource().getMemory(), 1024);
	EXPECT_EQ(it->getResource().getVirtualCores(), 1);
}

TEST_F(TestApplicationClient,TestGetClusterNodes){
	list<NodeState> states;
	list<NodeReport> reports = client->getClusterNodes(states);
	EXPECT_EQ(reports.size(), 1);
	list<NodeReport>::iterator it = reports.begin();
	EXPECT_EQ(it->getNodeId().getHost(), "node1");
	EXPECT_EQ(it->getNodeId().getPort(), 9983);
	EXPECT_EQ(it->getRackName(), "default-rack");
	EXPECT_EQ(it->getResourceCapability().getMemory(), 2048*8);
	EXPECT_EQ(it->getResourceCapability().getVirtualCores(), 8);
	EXPECT_EQ(it->getNodeState(), NodeState::NS_RUNNING);
	EXPECT_EQ(it->getNumContainers(), 8);
}

TEST_F(TestApplicationClient,TestGetQueueInfo){
	string queue = "";
	QueueInfo queueInfo = client->getQueueInfo(queue,true,true,true);
	EXPECT_EQ(queueInfo.getQueueName(), "default");
	EXPECT_FLOAT_EQ(queueInfo.getCapacity(), 0.67);
	EXPECT_FLOAT_EQ(queueInfo.getMaximumCapacity(), 0.95);
	EXPECT_FLOAT_EQ(queueInfo.getCurrentCapacity(), 0.5);
	EXPECT_EQ(queueInfo.getQueueState(), QueueState::Q_RUNNING);
	list<QueueInfo> child = queueInfo.getChildQueues();
	EXPECT_EQ(child.size(), 1);
	list<QueueInfo>::iterator it = child.begin();
	EXPECT_EQ(it->getQueueName(), "hawq-queue");
	EXPECT_FLOAT_EQ(it->getCapacity(), 0.33);
	EXPECT_FLOAT_EQ(it->getMaximumCapacity(), 0.5);
	EXPECT_FLOAT_EQ(it->getCurrentCapacity(), 0.25);
	list<ApplicationReport> appReportList = queueInfo.getApplicationReports();
	list<ApplicationReport>::iterator itAppReport = appReportList.begin();
	EXPECT_EQ(itAppReport->getApplicationId().getId(), 100);
	EXPECT_EQ(itAppReport->getUser(), "postgres");
}

TEST_F(TestApplicationClient,TestForceKillApplication){
	ApplicationId appId;
	client->forceKillApplication(appId);
}

TEST_F(TestApplicationClient,TestGetClusterMetrics){
	YarnClusterMetrics response = client->getClusterMetrics();
	EXPECT_EQ(response.getNumNodeManagers(), 10);
}

TEST_F(TestApplicationClient,TestGetApplications){
	list<string> applicationTypes;
	list<YarnApplicationState> applicationStates;
	list<ApplicationReport> reports = client->getApplications(applicationTypes,applicationStates);
	EXPECT_EQ(reports.size(), 1);
	list<ApplicationReport>::iterator it = reports.begin();
	EXPECT_EQ(it->getApplicationId().getId(), 100);
	EXPECT_EQ(it->getUser(), "postgres");
}

TEST_F(TestApplicationClient,TestGetQueueAclsInfo){
	list<QueueUserACLInfo> response = client->getQueueAclsInfo();
	EXPECT_EQ(response.size(), 1);
	list<QueueUserACLInfo>::iterator it = response.begin();
	EXPECT_EQ(it->getQueueName(), "default");
	list<QueueACL> queueACLs = it->getUserAcls();
	EXPECT_EQ(queueACLs.size(), 2);
	list<QueueACL>::iterator queueACL = queueACLs.begin();
	EXPECT_EQ(*queueACL, QueueACL::QACL_ADMINISTER_QUEUE);
	*queueACL++;
	EXPECT_EQ(*queueACL, QueueACL::QACL_SUBMIT_APPLICATIONS);
}

TEST_F(TestApplicationClient, TestRMInfo){
	string rmHost("localhost");
	string rmPort("8032");

	RMInfo rmInfo = RMInfo();
	rmInfo.setHost(rmHost);
	rmInfo.setPort(rmPort);
	EXPECT_EQ(rmInfo.getHost(), rmHost);
	EXPECT_EQ(rmInfo.getPort(), rmPort);
}
