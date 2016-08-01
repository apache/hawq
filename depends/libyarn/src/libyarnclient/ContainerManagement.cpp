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

#include "ContainerManagement.h"

#include "rpc/RpcAuth.h"
#include "common/XmlConfig.h"
#include "common/SessionConfig.h"
#include <sstream>

namespace libyarn {

ContainerManagement::ContainerManagement() {
#ifdef MOCKTEST
    stub = NULL;
#endif
}

ContainerManagement::~ContainerManagement() {
}

/*
 ---------------------
 message StartContainerRequestProto {
 optional ContainerLaunchContextProto container_launch_context = 1;
 optional hadoop.common.TokenProto container_token = 2;
 }

 message StartContainerResponseProto {
 repeated StringBytesMapProto services_meta_data = 1;
 }
 ---------------------
 rpc startContainers(StartContainersRequestProto) returns (StartContainersResponseProto);
 ---------------------
 message StartContainersRequestProto {
 repeated StartContainerRequestProto start_container_request = 1;
 }

 message StartContainersResponseProto {
 repeated StringBytesMapProto services_meta_data = 1;
 repeated ContainerIdProto succeeded_requests = 2;
 repeated ContainerExceptionMapProto failed_requests = 3;
 }
 */

StartContainerResponse ContainerManagement::startContainer(Container &container,
		StartContainerRequest &request, Token &nmToken) {
	//1. setup Connection to NodeManager
	string host = container.getNodeId().getHost();
	ostringstream oss;
	oss << container.getNodeId().getPort();
	string port(oss.str());

	LOG(DEBUG1,
			"ContainerManagement::startContainer, is going to connect to NM [%s:%s] to start container",
			host.c_str(), port.c_str());

	UserInfo user = UserInfo::LocalUser();
	Yarn::Token anotherToken;
	anotherToken.setIdentifier(nmToken.getIdentifier());
	anotherToken.setKind(nmToken.getKind());
	anotherToken.setPassword(nmToken.getPassword());
	anotherToken.setService(nmToken.getService());
	user.addToken(anotherToken);

	RpcAuth rpcAuth(user, AuthMethod::TOKEN);
	Yarn::Config config;
	SessionConfig sessionConfig(config);

#ifdef MOCKTEST
	 ContainerManagementProtocol *nmClient = stub->getContainerManagementProtocol();
#else
	 ContainerManagementProtocol *nmClient = new ContainerManagementProtocol(
			host, port, anotherToken.getService(), sessionConfig, rpcAuth);
#endif

	//2. startContainers
	StartContainersRequest scsRequest;
	list<StartContainerRequest> requests;
	requests.push_back(request);
	scsRequest.setStartContainerRequests(requests);

	StartContainersResponse scsResponse = nmClient->startContainers(scsRequest);

	StartContainerResponse scResponse;
	scResponse.setServicesMetaData(scsResponse.getServicesMetaData());

	LOG(DEBUG1,
			"ContainerManagement::startContainer, after start a container, id:%ld on NM [%s:%s]",
			container.getId().getId(), host.c_str(), port.c_str());

	//3. free
	delete nmClient;

	return scResponse;
}

/*
 rpc stopContainers(StopContainersRequestProto) returns (StopContainersResponseProto);

 message StopContainersRequestProto {
 repeated ContainerIdProto container_id = 1;
 }

 message StopContainersResponseProto {
 repeated ContainerIdProto succeeded_requests = 1;
 repeated ContainerExceptionMapProto failed_requests = 2;
 }
 */

void ContainerManagement::stopContainer(Container &container, Token &nmToken) {
	//1. setup Connection to NodeManager
	string host = container.getNodeId().getHost();
	ostringstream oss;
	oss << container.getNodeId().getPort();
	string port(oss.str());

	LOG(DEBUG1,
			"ContainerManagement::stopContainer, is going to connect to NM [%s:%s] to stop container",
			host.c_str(), port.c_str());

	UserInfo user = UserInfo::LocalUser();
	Yarn::Token anotherToken;
	anotherToken.setIdentifier(nmToken.getIdentifier());
	anotherToken.setKind(nmToken.getKind());
	anotherToken.setPassword(nmToken.getPassword());
	anotherToken.setService(nmToken.getService());
	user.addToken(anotherToken);

	RpcAuth rpcAuth(user, AuthMethod::TOKEN);
	Yarn::Config config;
	SessionConfig sessionConfig(config);

#ifdef MOCKTEST
	 ContainerManagementProtocol *nmClient = stub->getContainerManagementProtocol();
#else
	 ContainerManagementProtocol *nmClient = new ContainerManagementProtocol(
			host, port, anotherToken.getService(), sessionConfig, rpcAuth);
#endif

	//2. stopContainers
	ContainerId cid = container.getId();
	list<ContainerId> cids;
	cids.push_back(cid);

	StopContainersRequest request;
	request.setContainerIds(cids);
	nmClient->stopContainers(request);
	//3. free
	delete nmClient;
}

ContainerStatus ContainerManagement::getContainerStatus(Container &container,
		Token &nmToken) {
	//1. setup Connection to NodeManager
	string host = container.getNodeId().getHost();
	ostringstream oss;
	oss << container.getNodeId().getPort();
	string port(oss.str());

	LOG(DEBUG1,
			"ContainerManagement, is going to connect to NM [%s:%s] to getContainerStatus container",
			host.c_str(), port.c_str());

	UserInfo user = UserInfo::LocalUser();
	Yarn::Token anotherToken;
	anotherToken.setIdentifier(nmToken.getIdentifier());
	anotherToken.setKind(nmToken.getKind());
	anotherToken.setPassword(nmToken.getPassword());
	anotherToken.setService(nmToken.getService());
	user.addToken(anotherToken);

	RpcAuth rpcAuth(user, AuthMethod::TOKEN);
	Yarn::Config config;
	SessionConfig sessionConfig(config);

#ifdef MOCKTEST
	 ContainerManagementProtocol *nmClient = stub->getContainerManagementProtocol();
#else
	 ContainerManagementProtocol *nmClient = new ContainerManagementProtocol(
			host, port, anotherToken.getService(), sessionConfig, rpcAuth);
#endif

	ContainerId cid = container.getId();
	list<ContainerId> cids;
	cids.push_back(cid);

	GetContainerStatusesRequest request;
	request.setContainerIds(cids);

	GetContainerStatusesResponse response = nmClient->getContainerStatuses(request);

	//3. free
	delete nmClient;
	list<ContainerStatus> statusList = response.getContainerStatuses();
	if (statusList.size() > 0){
		list<ContainerStatus>::iterator statusHead = statusList.begin();
		return (*statusHead);
	}else{
		ContainerStatus status;
		return status;
	}

}

} /* namespace libyarn */
