/*
 * RegisterApplicationMasterRequest.cpp
 *
 *  Created on: Jul 15, 2014
 *      Author: bwang
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
