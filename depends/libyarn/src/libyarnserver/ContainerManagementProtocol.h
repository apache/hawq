/*
 * ContainerManagementProtocol.h
 *
 *  Created on: Jun 25, 2014
 *      Author: bwang
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

using namespace std;
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
	void invoke(const RpcCall & call);

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
