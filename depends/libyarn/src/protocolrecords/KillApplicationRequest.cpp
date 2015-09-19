/*
 * KillApplicationRequest.cpp
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#include "KillApplicationRequest.h"

namespace libyarn {

KillApplicationRequest::KillApplicationRequest() {
	requestProto = KillApplicationRequestProto::default_instance();
}

KillApplicationRequest::KillApplicationRequest(
		const KillApplicationRequestProto &proto) :
		requestProto(proto) {
}

KillApplicationRequest::~KillApplicationRequest() {
}

KillApplicationRequestProto& KillApplicationRequest::getProto() {
	return requestProto;
}

void KillApplicationRequest::setApplicationId(ApplicationID &applicationId) {
	ApplicationIdProto* appId = new ApplicationIdProto();
	appId->CopyFrom(applicationId.getProto());
	requestProto.set_allocated_application_id(appId);
}

} /* namespace libyarn */
