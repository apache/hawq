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

#include "MockApplicationClientProtocol.h"

using namespace testing;
using namespace Mock;
using std::string;

class TestMockApplicationClientProtocol: public ::testing::Test {
public:
	TestMockApplicationClientProtocol(){
		string user_name("postgres");
		string rmHost("localhost");
		string rmPort("8032");
		string tokenService = "";
		Yarn::Config config;
		Yarn::Internal::SessionConfig sessionConfig(config);
		Yarn::Internal::UserInfo user = Yarn::Internal::UserInfo::LocalUser();
		protocol = new MockApplicationClientProtocol(user_name, rmHost,rmPort,tokenService, sessionConfig);
}
	~TestMockApplicationClientProtocol(){
		delete protocol;
	}
protected:
	MockApplicationClientProtocol *protocol;
};

TEST_F(TestMockApplicationClientProtocol,TestGetNewApplication){
	GetNewApplicationResponseProto responseProto;
	EXPECT_CALL((*protocol),getNewApplication(_)).Times(AnyNumber()).WillOnce(Return(GetNewApplicationResponse(responseProto)));
	GetNewApplicationRequest request;
	GetNewApplicationResponse response = protocol->getNewApplication(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationClientProtocol,TestSubmitApplication){
	EXPECT_CALL((*protocol),submitApplication(_)).Times(AnyNumber()).WillOnce(Return());
	SubmitApplicationRequest request;
	protocol->submitApplication(request);
}

TEST_F(TestMockApplicationClientProtocol,TestGetApplicationReport){
	GetApplicationReportResponseProto responseProto;
	EXPECT_CALL((*protocol),getApplicationReport(_)).Times(AnyNumber()).WillOnce(Return(GetApplicationReportResponse(responseProto)));
	GetApplicationReportRequest request;
	GetApplicationReportResponse response = protocol->getApplicationReport(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationClientProtocol,TestGetContainers){
	GetContainersResponseProto responseProto;
	EXPECT_CALL((*protocol),getContainers(_)).Times(AnyNumber()).WillOnce(Return(GetContainersResponse(responseProto)));
	GetContainersRequest request;
	GetContainersResponse response = protocol->getContainers(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationClientProtocol,TestGetClusterNodes){
	GetClusterNodesResponseProto responseProto;
	EXPECT_CALL((*protocol),getClusterNodes(_)).Times(AnyNumber()).WillOnce(Return(GetClusterNodesResponse(responseProto)));
	GetClusterNodesRequest request;
	GetClusterNodesResponse response = protocol->getClusterNodes(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationClientProtocol,TestGetQueueInfo){
	GetQueueInfoResponseProto responseProto;
	EXPECT_CALL((*protocol),getQueueInfo(_)).Times(AnyNumber()).WillOnce(Return(GetQueueInfoResponse(responseProto)));
	GetQueueInfoRequest request;
	GetQueueInfoResponse response = protocol->getQueueInfo(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationClientProtocol,TestForceKillApplication){
	KillApplicationResponseProto responseProto;
	EXPECT_CALL((*protocol),forceKillApplication(_)).Times(AnyNumber()).WillOnce(Return(KillApplicationResponse(responseProto)));
	KillApplicationRequest request;
	KillApplicationResponse response = protocol->forceKillApplication(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationClientProtocol,TestGetClusterMetrics){
	GetClusterMetricsResponseProto responseProto;
	EXPECT_CALL((*protocol),getClusterMetrics(_)).Times(AnyNumber()).WillOnce(Return(GetClusterMetricsResponse(responseProto)));
	GetClusterMetricsRequest request;
	GetClusterMetricsResponse response = protocol->getClusterMetrics(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationClientProtocol,TestGetApplications){
	GetApplicationsResponseProto responseProto;
	EXPECT_CALL((*protocol),getApplications(_)).Times(AnyNumber()).WillOnce(Return(GetApplicationsResponse(responseProto)));
	GetApplicationsRequest request;
	GetApplicationsResponse response = protocol->getApplications(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}

TEST_F(TestMockApplicationClientProtocol,TestGetQueueAclsInfo){
	GetQueueUserAclsInfoResponseProto responseProto;
	EXPECT_CALL((*protocol),getQueueAclsInfo(_)).Times(AnyNumber()).WillOnce(Return(GetQueueUserAclsInfoResponse(responseProto)));
	GetQueueUserAclsInfoRequest request;
	GetQueueUserAclsInfoResponse response = protocol->getQueueAclsInfo(request);
	EXPECT_EQ(response.responseProto._cached_size_,0);
}



