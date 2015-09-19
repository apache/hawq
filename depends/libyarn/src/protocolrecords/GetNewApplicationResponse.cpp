/*
 * GetNewApplicationResponse.cpp
 *
 *  Created on: Jul 10, 2014
 *      Author: bwang
 */

#include "GetNewApplicationResponse.h"

namespace libyarn {

GetNewApplicationResponse::GetNewApplicationResponse() {
	this->responseProto = GetNewApplicationResponseProto::default_instance();
}

GetNewApplicationResponse::GetNewApplicationResponse(
		const GetNewApplicationResponseProto &proto) :
		responseProto(proto) {
}

GetNewApplicationResponse::~GetNewApplicationResponse() {
}

void GetNewApplicationResponse::setApplicationId(ApplicationID &appId) {
	ApplicationIdProto *proto = new ApplicationIdProto();
	proto->CopyFrom(appId.getProto());
	responseProto.set_allocated_application_id(proto);
}

ApplicationID GetNewApplicationResponse::getApplicationId() {
	return ApplicationID(responseProto.application_id());
}

void GetNewApplicationResponse::setResource(Resource &resource) {
	ResourceProto* rProto = new ResourceProto();
	rProto->CopyFrom(resource.getProto());
	responseProto.set_allocated_maximumcapability(rProto);
}

Resource GetNewApplicationResponse::getResource() {
	return Resource(responseProto.maximumcapability());
}

} /* namespace libyarn */
