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

#include "ContainerManagementProtocol.h"

#include "common/Exception.h"
#include "common/ExceptionInternal.h"

using namespace Yarn;

namespace libyarn {

ContainerManagementProtocol::ContainerManagementProtocol(std::string & nmHost,
        std::string & nmPort, const std::string & tokenService,
        const SessionConfig & c, const RpcAuth & a) :
        auth(a), client(RpcClient::getClient()), conf(c), protocol(
        CONTAINER_MANAGEMENT_VERSION, CONTAINER_MANAGEMENT_PROTOCOL,
        NM_TOKEN_KIND), server(tokenService, nmHost, nmPort) {
}

ContainerManagementProtocol::~ContainerManagementProtocol() {
}

void ContainerManagementProtocol::invoke(const RpcCall & call) {
    try {
        channel = &client.getChannel(auth, protocol, server, conf);
        channel->invoke(call);
        channel->close(false);
    } catch (...) {
        channel->close(false);
        throw;
    }
}


StartContainersResponse ContainerManagementProtocol::startContainers(StartContainersRequest &request) {
    try {
        StartContainersResponseProto responseProto;
        StartContainersRequestProto requestProto = request.getProto();
        invoke(RpcCall(true, "startContainers", &requestProto, &responseProto));
        return StartContainersResponse(responseProto);
    } catch (const YarnRpcServerException & e) {
        UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

StopContainersResponse ContainerManagementProtocol::stopContainers(StopContainersRequest &request) {
    try {
        StopContainersRequestProto requestProto = request.getProto();
        StopContainersResponseProto responseProto;
        invoke(RpcCall(true, "stopContainers", &requestProto, &responseProto));
        return StopContainersResponse(responseProto);
    } catch (const YarnRpcServerException & e) {
        UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

GetContainerStatusesResponse ContainerManagementProtocol::getContainerStatuses(GetContainerStatusesRequest &request){
    try {
        GetContainerStatusesRequestProto requestProto = request.getProto();
        GetContainerStatusesResponseProto responseProto;
        invoke(RpcCall(true, "getContainerStatuses", &requestProto, &responseProto));
        return GetContainerStatusesResponse(responseProto);
    } catch (const YarnRpcServerException & e) {
        UnWrapper<UnresolvedLinkException, YarnIOException> unwrapper(e);
        unwrapper.unwrap(__FILE__, __LINE__);
    }
}

} /* namespace libyarn */
