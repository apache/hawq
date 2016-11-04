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

#ifndef CONTAINERMANAGEMENTPROTOCOL_H_
#define CONTAINERMANAGEMENTPROTOCOL_H_

#include <cstdint>
#include <iostream>

#include "rpc/RpcAuth.h"
#include "rpc/RpcConfig.h"
#include "rpc/RpcProtocolInfo.h"
#include "rpc/RpcServerInfo.h"
#include "rpc/RpcClient.h"

#include "common/SessionConfig.h"

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"
#include "YARN_applicationmaster_protocol.pb.h"

#include "protocolrecords/StartContainersRequest.h"
#include "protocolrecords/StartContainersResponse.h"

#include "protocolrecords/StopContainersRequest.h"
#include "protocolrecords/StopContainersResponse.h"

#include "protocolrecords/GetContainerStatusesRequest.h"
#include "protocolrecords/GetContainerStatusesResponse.h"

#define CONTAINER_MANAGEMENT_VERSION 1
#define CONTAINER_MANAGEMENT_PROTOCOL "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB"
#define NM_TOKEN_KIND "NMToken"

using std::string; using std::list;
using namespace google::protobuf;

using namespace hadoop::yarn;
using namespace Yarn::Internal;

namespace libyarn {

class ContainerManagementProtocol {
public:
	ContainerManagementProtocol(std::string & nmHost, std::string & nmPort,
			const std::string & tokenService, const SessionConfig & c,
			const RpcAuth & a);

	virtual ~ContainerManagementProtocol();

	virtual StartContainersResponse startContainers(StartContainersRequest &request);

	virtual StopContainersResponse stopContainers(StopContainersRequest &request);

	virtual GetContainerStatusesResponse getContainerStatuses(GetContainerStatusesRequest &request);

private:
	virtual void invoke(const RpcCall & call);

private:
	RpcAuth auth;
	RpcClient & client;
	RpcConfig conf;
	RpcProtocolInfo protocol;
	RpcServerInfo server;
	RpcChannel *channel;
};


}/* namespace libyarn */

#endif /* CONTAINERMANAGEMENTPROTOCOL_H_ */
