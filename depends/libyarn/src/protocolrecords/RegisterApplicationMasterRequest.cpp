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


#include "RegisterApplicationMasterRequest.h"

namespace libyarn {

RegisterApplicationMasterRequest::RegisterApplicationMasterRequest() {
	requestProto = RegisterApplicationMasterRequestProto::default_instance();
}

RegisterApplicationMasterRequest::RegisterApplicationMasterRequest(
		const RegisterApplicationMasterRequestProto &proto) :
		requestProto(proto) {
}

RegisterApplicationMasterRequest::~RegisterApplicationMasterRequest() {
}

RegisterApplicationMasterRequestProto& RegisterApplicationMasterRequest::getProto(){
	return requestProto;
}

void RegisterApplicationMasterRequest::setHost(string &host) {
	requestProto.set_host(host);
}

string RegisterApplicationMasterRequest::getHost() {
	return requestProto.host();
}

void RegisterApplicationMasterRequest::setRpcPort(int32_t port) {
	requestProto.set_rpc_port(port);
}

int32_t RegisterApplicationMasterRequest::getRpcPort() {
	return requestProto.rpc_port();
}

void RegisterApplicationMasterRequest::setTrackingUrl(string &url) {
	requestProto.set_tracking_url(url);
}

string RegisterApplicationMasterRequest::getTrackingUrl() {
	return requestProto.tracking_url();
}

} /* namespace libyarn */
