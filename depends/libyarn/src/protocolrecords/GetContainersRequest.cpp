/*
 * GetContainersRequest.cpp
 *
 *  Created on: Jan 5, 2015
 *      Author: weikui
 */

#include "GetContainersRequest.h"

namespace libyarn {

GetContainersRequest::GetContainersRequest() {
	requestProto = GetContainersRequestProto::default_instance();
}

GetContainersRequest::GetContainersRequest(
		const GetContainersRequestProto &proto) :
		requestProto(proto) {
}

GetContainersRequest::~GetContainersRequest() {
}

GetContainersRequestProto& GetContainersRequest::getProto() {
	return requestProto;
}

void GetContainersRequest::setApplicationAttemptId(ApplicationAttemptId &appAttemptId) {
	ApplicationAttemptIdProto *proto = new ApplicationAttemptIdProto();
	proto->CopyFrom(appAttemptId.getProto());
	requestProto.set_allocated_app_attempt_id(proto);
}

ApplicationAttemptId GetContainersRequest::getApplicationAttemptId() {
	return ApplicationAttemptId(requestProto.app_attempt_id());
}

} /* namespace libyarn */





