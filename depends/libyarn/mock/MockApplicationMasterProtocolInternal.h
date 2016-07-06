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
 * MockApplicationMasterProtocolInternal.h
 *
 *  Created on: May 4, 2016
 *      Author: Yangcheng Luo
 */

#ifndef MOCKAPPLICATIONMASTERPROTOCOLINTERNAL_H_
#define MOCKAPPLICATIONMASTERPROTOCOLINTERNAL_H_
#include <string>

#include "gmock/gmock.h"
#include "libyarnserver/ApplicationMasterProtocol.h"

using namespace libyarn;
using std::string;

namespace Mock{
class MockApplicationMasterProtocolInternal : public ApplicationMasterProtocol {
public:
	MockApplicationMasterProtocolInternal(const string & schedHost, const string & schedPort,
			const string & tokenService, const SessionConfig & c, const RpcAuth & a):
			ApplicationMasterProtocol(schedHost, schedPort, tokenService, c, a){
	}
	~MockApplicationMasterProtocolInternal(){
	}

	MOCK_METHOD1(invoke, void(const RpcCall & call));

};
}
#endif /* MOCKAPPLICATIONCLIENTPROTOCOLINTERNAL_H_ */
