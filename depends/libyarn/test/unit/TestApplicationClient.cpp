/*
 * TestApplicationClient.cpp
 *
 *  Created on: Mar 6, 2015
 *      Author: weikui
 */
#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "libyarnclient/ApplicationClient.h"
#include "MockApplicationClientProtocol.h"

using namespace std;
using namespace libyarn;
using namespace testing;
using namespace Mock;

class TestApplicationClient: public ::testing::Test {
public:
	TestApplicationClient(){
		string rmHost("localhost");
		string rmPort("8032");
		string tokenService = "";
		Yarn::Config config;
		Yarn::Internal::SessionConfig sessionConfig(config);
		Yarn::Internal::UserInfo user = Yarn::Internal::UserInfo::LocalUser();
		Yarn::Internal::RpcAuth rpcAuth(user, Yarn::Internal::AuthMethod::SIMPLE);
		MockApplicationClientProtocol *protocol = new MockApplicationClientProtocol(rmHost,rmPort,tokenService, sessionConfig,rpcAuth);

		GetNewApplicationResponseProto responseProto;
		EXPECT_CALL((*protocol),getNewApplication(_)).Times(AnyNumber()).WillOnce(Return(GetNewApplicationResponse(responseProto)));
		EXPECT_CALL((*protocol),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());

		GetApplicationReportResponseProto applicationReportResponseProto;
		EXPECT_CALL((*protocol),getApplicationReport(_)).Times(AnyNumber()).WillOnce(Return(GetApplicationReportResponse(applicationReportResponseProto)));

		GetContainersResponseProto containersResponseProto;
		EXPECT_CALL((*protocol),getContainers(_)).Times(AnyNumber()).WillOnce(Return(GetContainersResponse(containersResponseProto)));

		GetClusterNodesResponseProto clusterNodesResponseProto;
		EXPECT_CALL((*protocol),getClusterNodes(_)).Times(AnyNumber()).WillOnce(Return(GetClusterNodesResponse(clusterNodesResponseProto)));

		GetQueueInfoResponseProto queueInfoResponseProto;
		EXPECT_CALL((*protocol),getQueueInfo(_)).Times(AnyNumber()).WillOnce(Return(GetQueueInfoResponse(queueInfoResponseProto)));

		KillApplicationResponseProto killApplicationResponseProto;
		EXPECT_CALL((*protocol),forceKillApplication(_)).Times(AnyNumber()).WillOnce(Return(KillApplicationResponse(killApplicationResponseProto)));

		GetClusterMetricsResponseProto clusterMetricsResponseProto;
		EXPECT_CALL((*protocol),getClusterMetrics(_)).Times(AnyNumber()).WillOnce(Return(GetClusterMetricsResponse(clusterMetricsResponseProto)));

		GetApplicationsResponseProto applicationsResponseProto;
		EXPECT_CALL((*protocol),getApplications(_)).Times(AnyNumber()).WillOnce(Return(GetApplicationsResponse(applicationsResponseProto)));

		GetQueueUserAclsInfoResponseProto queueUserAclsInfoResponseProto;
		EXPECT_CALL((*protocol),getQueueAclsInfo(_)).Times(AnyNumber()).WillOnce(Return(GetQueueUserAclsInfoResponse(queueUserAclsInfoResponseProto)));

		client = new ApplicationClient(protocol);
	}
	~TestApplicationClient(){
		delete client;
	}
protected:
	ApplicationClient *client;
};

TEST_F(TestApplicationClient,TestGetNewApplication){
	ApplicationID response = client->getNewApplication();
	EXPECT_EQ(response.getProto().id_,0);
}

TEST_F(TestApplicationClient,TestSubmitApplication){
	ApplicationSubmissionContext appContext;
	client->submitApplication(appContext);
}

TEST_F(TestApplicationClient,TestGetApplicationReport){
	ApplicationID appId;
	ApplicationReport report = client->getApplicationReport(appId);
	EXPECT_EQ(report.reportProto._cached_size_,0);
}

TEST_F(TestApplicationClient,TestGetContainers){
	ApplicationAttemptId appAttempId;
	list<ContainerReport> reports = client->getContainers(appAttempId);
	EXPECT_EQ(reports.size(),0);
}

TEST_F(TestApplicationClient,TestGetClusterNodes){
	list<NodeState> states;
	list<NodeReport> reports = client->getClusterNodes(states);
	EXPECT_EQ(reports.size(),0);
}

TEST_F(TestApplicationClient,TestGetQueueInfo){
	string queue = "";
	QueueInfo queueInfo = client->getQueueInfo(queue,true,true,true);
	EXPECT_EQ(queueInfo.infoProto._cached_size_,0);
}

TEST_F(TestApplicationClient,TestForceKillApplication){
	ApplicationID appId;
	client->forceKillApplication(appId);
}

TEST_F(TestApplicationClient,TestGetClusterMetrics){
	YarnClusterMetrics response = client->getClusterMetrics();
	EXPECT_EQ(response.metricsProto._cached_size_,0);
}

TEST_F(TestApplicationClient,TestGetApplications){
	list<string> applicationTypes;
	list<YarnApplicationState> applicationStates;
	list<ApplicationReport> reports = client->getApplications(applicationTypes,applicationStates);
	EXPECT_EQ(reports.size(),0);
}

TEST_F(TestApplicationClient,TestGetQueueAclsInfo){
	list<QueueUserACLInfo> response = client->getQueueAclsInfo();
	EXPECT_EQ(response.size(),0);
}
