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

#include "rpc/RpcAuth.h"
#include "common/SessionConfig.h"
#include "common/Exception.h"

#include "protocolrecords/GetNewApplicationRequest.h"
#include "protocolrecords/GetNewApplicationResponse.h"
#include "protocolrecords/SubmitApplicationRequest.h"
#include "protocolrecords/GetApplicationReportRequest.h"
#include "protocolrecords/GetApplicationReportResponse.h"
#include "protocolrecords/GetContainersRequest.h"
#include "protocolrecords/GetContainersResponse.h"
#include "protocolrecords/GetClusterNodesRequest.h"
#include "protocolrecords/GetClusterNodesResponse.h"
#include "protocolrecords/GetQueueInfoRequest.h"
#include "protocolrecords/GetQueueInfoResponse.h"
#include "protocolrecords/GetClusterMetricsRequest.h"
#include "protocolrecords/GetClusterMetricsResponse.h"
#include "protocolrecords/KillApplicationRequest.h"
#include "protocolrecords/KillApplicationResponse.h"
#include "protocolrecords/GetApplicationsRequest.h"
#include "protocolrecords/GetApplicationsResponse.h"
#include "protocolrecords/GetQueueUserAclsInfoRequest.h"
#include "protocolrecords/GetQueueUserAclsInfoResponse.h"

#include "libyarnserver/ApplicationClientProtocol.h"
#include "MockApplicationClientProtocolInternal.h"

using std::string;
using Yarn::Internal::RpcAuth;
using Yarn::Internal::SessionConfig;
using Yarn::Config;
using namespace libyarn;
using namespace testing;
using namespace Mock;
using namespace Yarn;

class TestApplicationClientProtocol: public ::testing::Test {
public:
	TestApplicationClientProtocol():
		user("postgres"), rmHost("localhost"), rmPort("8032"), sc(conf){}
	~TestApplicationClientProtocol(){
	}
protected:
	const string user;
	const string rmHost;
	const string rmPort;
	const string tokenService;
	const Config conf;
	const SessionConfig sc;
};

TEST_F(TestApplicationClientProtocol, TestGetNewApplicationException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	GetNewApplicationRequest gnareq;
	GetNewApplicationResponse gnares;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(gnares = macp.getNewApplication(gnareq), YarnFailoverException);
	EXPECT_THROW(gnares = macp.getNewApplication(gnareq), YarnIOException);
	EXPECT_THROW(gnares = macp.getNewApplication(gnareq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestSubmitApplicationException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	SubmitApplicationRequest sareq;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.submitApplication(sareq), YarnFailoverException);
	EXPECT_THROW(macp.submitApplication(sareq), YarnIOException);
	EXPECT_THROW(macp.submitApplication(sareq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestGetApplicationReportException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	GetApplicationReportRequest garreq;
	GetApplicationReportResponse garres;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.getApplicationReport(garreq), YarnFailoverException);
	EXPECT_THROW(macp.getApplicationReport(garreq), YarnIOException);
	EXPECT_THROW(macp.getApplicationReport(garreq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestGetContainersException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	GetContainersRequest gcreq;
	GetContainersResponse gcres;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.getContainers(gcreq), YarnFailoverException);
	EXPECT_THROW(macp.getContainers(gcreq), YarnIOException);
	EXPECT_THROW(macp.getContainers(gcreq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestGetClusterNodesException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	GetClusterNodesRequest gcnreq;
	GetClusterNodesResponse gcnres;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.getClusterNodes(gcnreq), YarnFailoverException);
	EXPECT_THROW(macp.getClusterNodes(gcnreq), YarnIOException);
	EXPECT_THROW(macp.getClusterNodes(gcnreq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestGetQueueInfoException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	GetQueueInfoRequest gqireq;
	GetQueueInfoResponse gqires;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.getQueueInfo(gqireq), YarnFailoverException);
	EXPECT_THROW(macp.getQueueInfo(gqireq), YarnIOException);
	EXPECT_THROW(macp.getQueueInfo(gqireq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestGetClusterMetricsException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	GetClusterMetricsRequest gcmreq;
	GetClusterMetricsResponse gcmres;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.getClusterMetrics(gcmreq), YarnFailoverException);
	EXPECT_THROW(macp.getClusterMetrics(gcmreq), YarnIOException);
	EXPECT_THROW(macp.getClusterMetrics(gcmreq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestForceKillApplicationException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	KillApplicationRequest kareq;
	KillApplicationResponse kares;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.forceKillApplication(kareq), YarnFailoverException);
	EXPECT_THROW(macp.forceKillApplication(kareq), YarnIOException);
	EXPECT_THROW(macp.forceKillApplication(kareq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestGetApplicationsException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	GetApplicationsRequest gareq;
	GetApplicationsResponse gares;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.getApplications(gareq), YarnFailoverException);
	EXPECT_THROW(macp.getApplications(gareq), YarnIOException);
	EXPECT_THROW(macp.getApplications(gareq), YarnException);
}

TEST_F(TestApplicationClientProtocol, TestGetQueueAclsInfoException){
	MockApplicationClientProtocolInternal macp(user, rmHost, rmPort, tokenService, sc);
	GetQueueUserAclsInfoRequest gquareq;
	GetQueueUserAclsInfoResponse gquares;

	EXPECT_CALL(macp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(macp.getQueueAclsInfo(gquareq), YarnFailoverException);
	EXPECT_THROW(macp.getQueueAclsInfo(gquareq), YarnIOException);
	EXPECT_THROW(macp.getQueueAclsInfo(gquareq), YarnException);
}
