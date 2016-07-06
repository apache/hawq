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
 * MockApplicationClientProtocolInternal.h
 *
 *  Created on: April 27, 2016
 *      Author: Yangcheng Luo
 */

#ifndef MOCKAPPLICATIONCLIENTPROTOCOLINTERNAL_H_
#define MOCKAPPLICATIONCLIENTPROTOCOLINTERNAL_H_
#include <string>

#include "gmock/gmock.h"
#include "libyarnserver/ApplicationClientProtocol.h"

using namespace libyarn;
using std::string;

namespace Mock{
class MockApplicationClientProtocolInternal : public ApplicationClientProtocol {
public:
	MockApplicationClientProtocolInternal(const string & user, const string & nmHost, const string & nmPort,
			const string & tokenService, const SessionConfig & c):
			ApplicationClientProtocol(user, nmHost, nmPort, tokenService, c){
	}
	~MockApplicationClientProtocolInternal(){
	}

	MOCK_METHOD1(invoke, void(const RpcCall & call));

};
}
#endif /* MOCKAPPLICATIONCLIENTPROTOCOLINTERNAL_H_ */
