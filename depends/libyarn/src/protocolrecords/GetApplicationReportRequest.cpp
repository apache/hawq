/*
 * GetApplicationReportRequest.cpp
 *
 *  Created on: Jul 14, 2014
 *      Author: bwang
 */

#include "GetApplicationReportRequest.h"

namespace libyarn {

GetApplicationReportRequest::GetApplicationReportRequest() {
	requestProto = GetApplicationReportRequestProto::default_instance();
}

GetApplicationReportRequest::GetApplicationReportRequest(
		const GetApplicationReportRequestProto &proto) :
		requestProto(proto) {
}

GetApplicationReportRequest::~GetApplicationReportRequest() {
}

GetApplicationReportRequestProto& GetApplicationReportRequest::getProto() {
	return requestProto;
}

void GetApplicationReportRequest::setApplicationId(ApplicationID &appId) {
	ApplicationIdProto* proto = new ApplicationIdProto();
	proto->CopyFrom(appId.getProto());
	requestProto.set_allocated_application_id(proto);
}

ApplicationID GetApplicationReportRequest::getApplicationId() {
	return ApplicationID(requestProto.application_id());
}

} /* namespace libyarn */
