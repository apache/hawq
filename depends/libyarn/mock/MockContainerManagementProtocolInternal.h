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
 * MockContainerManagementProtocolInternal.h
 *
 *  Created on: April 22, 2016
 *      Author: Ruilong Huo
 */

#ifndef MOCKCONTAINERMANAGEMENTPROTOCOLINTERNAL_H_
#define MOCKCONTAINERMANAGEMENTPROTOCOLINTERNAL_H_
#include <string>

#include "gmock/gmock.h"
#include "libyarnserver/ContainerManagementProtocol.h"

using namespace libyarn;
using std::string; using std::list;

namespace Mock{
class MockContainerManagementProtocolInternal : public ContainerManagementProtocol {
public:
	MockContainerManagementProtocolInternal(string & nmHost, string & nmPort,
			const string & tokenService, const SessionConfig & c,
			const RpcAuth & a):
			ContainerManagementProtocol(nmHost,nmPort,tokenService, c,a){
	}
	~MockContainerManagementProtocolInternal(){
	}

	MOCK_METHOD1(invoke, void(const RpcCall & call));

};
}
#endif /* MOCKCONTAINERMANAGEMENTPROTOCOLINTERNAL_H_ */
