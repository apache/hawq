/*
 * RegisterApplicationResponse.cpp
 *
 *  Created on: Jul 15, 2014
 *      Author: bwang
 */

#include "RegisterApplicationMasterResponse.h"

namespace libyarn {

RegisterApplicationMasterResponse::RegisterApplicationMasterResponse() {
	responseProto = RegisterApplicationMasterResponseProto::default_instance();
}

RegisterApplicationMasterResponse::RegisterApplicationMasterResponse(
		const RegisterApplicationMasterResponseProto &proto) :
		responseProto(proto) {
}

RegisterApplicationMasterResponse::~RegisterApplicationMasterResponse() {
}

RegisterApplicationMasterResponseProto& RegisterApplicationMasterResponse::getProto() {
	return responseProto;
}

void RegisterApplicationMasterResponse::setMaximumResourceCapability(
		Resource &capability) {
	ResourceProto* rProto = new ResourceProto();
	rProto->CopyFrom(capability.getProto());
	responseProto.set_allocated_maximumcapability(rProto);
}

Resource RegisterApplicationMasterResponse::getMaximumResourceCapability() {
	return Resource(responseProto.maximumcapability());
}

void RegisterApplicationMasterResponse::setClientToAMTokenMasterKey(
		string &key) {
	responseProto.set_client_to_am_token_master_key(key);
}

string RegisterApplicationMasterResponse::getClientToAMTokenMasterKey() {
	return responseProto.client_to_am_token_master_key();
}

void RegisterApplicationMasterResponse::setApplicationACLs(
		list<ApplicationACLMap> &aclMapList) {
	list<ApplicationACLMap>::iterator it = aclMapList.begin();
	for (; it != aclMapList.end(); it++) {
		ApplicationACLMapProto* aclMapProto =
				responseProto.add_application_acls();
		aclMapProto->CopyFrom((*it).getProto());
	}
}

list<ApplicationACLMap> RegisterApplicationMasterResponse::getApplicationACLs() {
	list<ApplicationACLMap> list;
	for (int i = 0; i < responseProto.application_acls_size(); i++) {
		list.push_back(ApplicationACLMap(responseProto.application_acls(i)));
	}
	return list;
}

} /* namespace libyarn */
