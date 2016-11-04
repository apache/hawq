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

#include "ApplicationMasterProtocol.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "rpc/RpcCall.h"
#include "rpc/RpcChannel.h"
#include "libyarncommon/RpcHelper.h"

#include "common/Exception.h"
#include "common/ExceptionInternal.h"

using namespace Yarn;

namespace libyarn {

ApplicationMasterProtocol::ApplicationMasterProtocol(const std::string & schedHost,
        const std::string & schedPort, const std::string & tokenService,
        const SessionConfig & c, const RpcAuth & a) :
        auth(a), client(RpcClient::getClient()), conf(c), protocol(
        APPLICATION_MASTER_VERSION, APPLICATION_MASTER_PROTOCOL,
        AMRM_TOKEN_KIND), server(tokenService, schedHost, schedPort) {
}

ApplicationMasterProtocol::~ApplicationMasterProtocol() {
}

void ApplicationMasterProtocol::invoke(const RpcCall & call) {
    try {
        channel = &client.getChannel(auth, protocol, server, conf);
        channel->invoke(call);
        channel->close(false);
    } catch (...) {
        channel->close(false);
        throw;
    }
}

RegisterApplicationMasterResponse ApplicationMasterProtocol::registerApplicationMaster(
        RegisterApplicationMasterRequest &request) {
    try {
        RegisterApplicationMasterResponseProto responseProto;
        RegisterApplicationMasterRequestProto requestProto = request.getProto();
        invoke(RpcCall(true, "registerApplicationMaster", &requestProto, &responseProto));
        return RegisterApplicationMasterResponse(responseProto);
    } catch (const YarnRpcServerException & e) {
        UnWrapper<ApplicationMasterNotRegisteredException, YarnIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}


AllocateResponse ApplicationMasterProtocol::allocate(AllocateRequest &request) {
    try {
        AllocateRequestProto requestProto = request.getProto();
        AllocateResponseProto responseProto;
        invoke(RpcCall(true, "allocate", &requestProto, &responseProto));
        return AllocateResponse(responseProto);
    } catch (const YarnRpcServerException & e) {
        UnWrapper<ApplicationMasterNotRegisteredException, YarnIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

FinishApplicationMasterResponse ApplicationMasterProtocol::finishApplicationMaster(
        FinishApplicationMasterRequest &request) {
    try {
        FinishApplicationMasterRequestProto requestProto = request.getProto();
        FinishApplicationMasterResponseProto responseProto;
        invoke(RpcCall(true, "finishApplicationMaster", &requestProto, &responseProto));
        return FinishApplicationMasterResponse(responseProto);
    } catch (const YarnRpcServerException & e) {
        UnWrapper<ApplicationMasterNotRegisteredException, YarnIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

}
