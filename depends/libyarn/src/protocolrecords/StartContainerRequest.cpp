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

#include "StartContainerRequest.h"

namespace libyarn {

StartContainerRequest::StartContainerRequest() {
	requestProto = StartContainerRequestProto::default_instance();
}

StartContainerRequest::StartContainerRequest(
		const StartContainerRequestProto &proto) :
		requestProto(proto) {
}

StartContainerRequest::~StartContainerRequest() {
}


StartContainerRequestProto& StartContainerRequest::getProto() {
	return requestProto;
}


void StartContainerRequest::setContainerLaunchCtx(ContainerLaunchContext &containerLaunchCtx) {
	ContainerLaunchContextProto* proto = new ContainerLaunchContextProto();
	proto->CopyFrom(containerLaunchCtx.getProto());
	requestProto.set_allocated_container_launch_context(proto);
}

ContainerLaunchContext StartContainerRequest::getContainerLaunchCtx() {
	return ContainerLaunchContext(requestProto.container_launch_context());
}

void StartContainerRequest::setContainerToken(Token &token) {
	TokenProto *proto = new TokenProto();
	proto->CopyFrom(token.getProto());
	requestProto.set_allocated_container_token(proto);
}

Token StartContainerRequest::getContainerToken() {
	return Token(requestProto.container_token());
}

} /* namespace libyarn */
