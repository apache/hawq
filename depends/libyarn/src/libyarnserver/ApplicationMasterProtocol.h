/*
 * ApplicationMasterProtocol.h
 *
 *  Created on: Jun 15, 2014
 *      Author: bwang
 */

#ifndef APPLICATIONMASTERPROTOCOL_H_
#define APPLICATIONMASTERPROTOCOL_H_

#include <iostream>
#include <list>

#include "rpc/RpcAuth.h"
#include "rpc/RpcConfig.h"
#include "rpc/RpcProtocolInfo.h"
#include "rpc/RpcServerInfo.h"
#include "rpc/RpcClient.h"

#include "common/SessionConfig.h"

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"
#include "YARN_applicationmaster_protocol.pb.h"

#include "protocolrecords/RegisterApplicationMasterRequest.h"
#include "protocolrecords/RegisterApplicationMasterResponse.h"
#include "protocolrecords/AllocateRequest.h"
#include "protocolrecords/AllocateResponse.h"

#include "protocolrecords/FinishApplicationMasterRequest.h"
#include "protocolrecords/FinishApplicationMasterResponse.h"

#define APPLICATION_MASTER_VERSION 1
#define APPLICATION_MASTER_PROTOCOL "org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB"
#define AMRM_TOKEN_KIND "YARN_AM_RM_TOKEN"

using namespace std;
using namespace google::protobuf;

using namespace hadoop::yarn;
using namespace Yarn::Internal;


namespace libyarn {

class ApplicationMasterProtocol {
public:
	ApplicationMasterProtocol(const std::string & schedHost, const std::string & schedPort,
			const std::string & tokenService, const SessionConfig & c,
			const RpcAuth & a);

	virtual ~ApplicationMasterProtocol();

	virtual RegisterApplicationMasterResponse registerApplicationMaster(
			RegisterApplicationMasterRequest &request);

	virtual AllocateResponse allocate(AllocateRequest &request);

	virtual FinishApplicationMasterResponse finishApplicationMaster(
			FinishApplicationMasterRequest &request);

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

}
#endif /* APPLICATIONMASTERPROTOCOL_H_ */
