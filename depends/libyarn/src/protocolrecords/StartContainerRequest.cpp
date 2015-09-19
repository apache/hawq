/*
 * StartContainerRequest.cpp
 *
 *  Created on: Jul 17, 2014
 *      Author: bwang
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
