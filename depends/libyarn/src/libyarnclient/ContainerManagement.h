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

#ifndef CONTAINERMANAGEMENT_H_
#define CONTAINERMANAGEMENT_H_

#include <cstdint>
#include <iostream>
#include <list>

#include "libyarncommon/UserInfo.h"
#include "libyarncommon/Token.h"
#include "libyarnserver/ContainerManagementProtocol.h"
#include "common/SessionConfig.h"
#include "records/Container.h"
#include "records/ContainerLaunchContext.h"
#include "records/ContainerId.h"
#include "records/NodeId.h"
#include "records/ContainerStatus.h"

#include "protocolrecords/StartContainerResponse.h"
#include "protocolrecords/StartContainerRequest.h"

#include "protocolrecords/StopContainersRequest.h"
#include "protocolrecords/StopContainersResponse.h"

#include "protocolrecords/GetContainerStatusesRequest.h"
#include "protocolrecords/GetContainerStatusesResponse.h"

using std::string; using std::list;

#ifdef MOCKTEST
#include "TestContainerManagementStub.h"
#endif

namespace libyarn {

class ContainerManagement {
public:
	ContainerManagement();

	virtual ~ContainerManagement();

	virtual StartContainerResponse startContainer(Container &container,
			StartContainerRequest &request, Token &nmToken);

	virtual void stopContainer(Container &container, Token &nmToken);

	virtual ContainerStatus getContainerStatus(Container &container, Token &nmToken);

#ifdef MOCKTEST
private:
    /*
     * for test
     */
    Mock::TestContainerManagementStub* stub;
#endif
};

} /* namespace libyarn */

#endif /* CONTAINERMANAGEMENT_H_ */
