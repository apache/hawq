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

/*
 * MockApplicationMasterProtocol.h
 *
 *  Created on: Mar 6, 2015
 *      Author: weikui
 */

#ifndef MOCKAPPLICATIONMASTERPROTOCOL_H_
#define MOCKAPPLICATIONMASTERPROTOCOL_H_
#include "gmock/gmock.h"
#include "libyarnserver/ApplicationMasterProtocol.h"

using namespace libyarn;
using std::string; using std::list;

namespace Mock{
class MockApplicationMasterProtocol : public ApplicationMasterProtocol {
public:
	MockApplicationMasterProtocol(string & schedHost,
			string & schedPort, const string & tokenService,
			const SessionConfig & c, const RpcAuth & a):
			ApplicationMasterProtocol(schedHost,schedPort,tokenService, c,a){
	}
	~MockApplicationMasterProtocol(){
	}

	MOCK_METHOD1(registerApplicationMaster, RegisterApplicationMasterResponse(RegisterApplicationMasterRequest &request));
	MOCK_METHOD1(allocate, AllocateResponse(AllocateRequest &request));
	MOCK_METHOD1(finishApplicationMaster, FinishApplicationMasterResponse(FinishApplicationMasterRequest &request));

};
}
#endif /* MOCKAPPLICATIONMASTERPROTOCOL_H_ */
