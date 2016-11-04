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

#include "protocolrecords/RegisterApplicationMasterRequest.h"
#include "protocolrecords/RegisterApplicationMasterResponse.h"
#include "protocolrecords/AllocateRequest.h"
#include "protocolrecords/AllocateResponse.h"
#include "protocolrecords/FinishApplicationMasterRequest.h"
#include "protocolrecords/FinishApplicationMasterResponse.h"

#include "libyarnserver/ApplicationMasterProtocol.h"
#include "MockApplicationMasterProtocolInternal.h"

using std::string;
using Yarn::Internal::RpcAuth;
using Yarn::Internal::SessionConfig;
using Yarn::Config;
using namespace libyarn;
using namespace testing;
using namespace Mock;
using namespace Yarn;

class TestApplicationMasterProtocol: public ::testing::Test {
public:
	TestApplicationMasterProtocol():
		schedHost("localhost"), schedPort("8030"), sc(conf){}
	~TestApplicationMasterProtocol(){
	}
protected:
	const string schedHost;
	const string schedPort;
	const string tokenService;
	const RpcAuth ra;
	const Config conf;
	const SessionConfig sc;
};

TEST_F(TestApplicationMasterProtocol, TestRegisterApplicationMasterException){
	MockApplicationMasterProtocolInternal mamp(schedHost, schedPort, tokenService, sc, ra);
	RegisterApplicationMasterRequest ramreq;
	RegisterApplicationMasterResponse ramres;

	EXPECT_CALL(mamp, invoke(_)).Times(1).WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(ramres = mamp.registerApplicationMaster(ramreq), YarnException);
}

TEST_F(TestApplicationMasterProtocol, TestAllocateException){
	MockApplicationMasterProtocolInternal mamp(schedHost, schedPort, tokenService, sc, ra);
	AllocateRequest areq;
	AllocateResponse ares;

	EXPECT_CALL(mamp, invoke(_)).Times(1).WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));

	EXPECT_THROW(ares = mamp.allocate(areq), YarnException);
}

TEST_F(TestApplicationMasterProtocol, TestFinishApplicationMasterException){
	MockApplicationMasterProtocolInternal mamp(schedHost, schedPort, tokenService, sc, ra);
	FinishApplicationMasterRequest famreq;
	FinishApplicationMasterResponse famres;

	EXPECT_CALL(mamp, invoke(_)).Times(3).WillOnce(Throw(YarnFailoverException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnRpcServerException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())))
			.WillOnce(Throw(YarnException("", __FILE__, __LINE__, Yarn::Internal::PrintStack(1, STACK_DEPTH).c_str())));


	EXPECT_THROW(famres = mamp.finishApplicationMaster(famreq), YarnFailoverException);
	EXPECT_THROW(famres = mamp.finishApplicationMaster(famreq), YarnIOException);
	EXPECT_THROW(famres = mamp.finishApplicationMaster(famreq), YarnException);
}
