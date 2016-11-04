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


#include "protocolrecords/StartContainersRequest.h"
#include "protocolrecords/StartContainersResponse.h"
#include "protocolrecords/StopContainersRequest.h"
#include "protocolrecords/StopContainersResponse.h"
#include "protocolrecords/GetContainerStatusesRequest.h"
#include "protocolrecords/GetContainerStatusesResponse.h"

#include "libyarnserver/ContainerManagementProtocol.h"
#include "MockContainerManagementProtocolInternal.h"

using std::string;

using Yarn::Internal::RpcAuth;
using Yarn::Internal::SessionConfig;
using Yarn::Config;
using namespace libyarn;
using namespace testing;
using namespace Mock;
using namespace Yarn;

class TestContainerManagementProtocol: public ::testing::Test {
public:
	TestContainerManagementProtocol():
		nmHost("localhost"), nmPort("8032"), tokenService(""), sc(conf){}
	~TestContainerManagementProtocol(){
	}
protected:
	string nmHost;
	string nmPort;
	const string tokenService;
	const RpcAuth ra;
	const Config conf;
	const SessionConfig sc;
};

TEST_F(TestContainerManagementProtocol,TestStartContainersException){
	MockContainerManagementProtocolInternal mcmp(nmHost, nmPort, tokenService, sc, ra);

	StartContainersRequest screq;
	StartContainersResponse scres;

	EXPECT_CALL(mcmp, invoke(_)).Times(2).WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(scres = mcmp.startContainers(screq), YarnIOException);
	EXPECT_THROW(scres = mcmp.startContainers(screq), YarnException);
}

TEST_F(TestContainerManagementProtocol,TestStopContainersException){
	MockContainerManagementProtocolInternal mcmp(nmHost, nmPort, tokenService, sc, ra);
	StopContainersRequest screq;
	StopContainersResponse scres;

	EXPECT_CALL(mcmp, invoke(_)).Times(2).WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(scres = mcmp.stopContainers(screq), YarnIOException);
	EXPECT_THROW(scres = mcmp.stopContainers(screq), YarnException);
}


TEST_F(TestContainerManagementProtocol,getContainerStatusesException){
	MockContainerManagementProtocolInternal mcmp(nmHost, nmPort, tokenService, sc, ra);
	GetContainerStatusesRequest gcsreq;
	GetContainerStatusesResponse gcsres;

	EXPECT_CALL(mcmp, invoke(_)).Times(2).WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(gcsres = mcmp.getContainerStatuses(gcsreq), YarnIOException);
	EXPECT_THROW(gcsres = mcmp.getContainerStatuses(gcsreq), YarnException);
}
