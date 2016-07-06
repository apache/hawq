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

using std::string; using std::list;
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
	virtual void invoke(const RpcCall & call);

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
