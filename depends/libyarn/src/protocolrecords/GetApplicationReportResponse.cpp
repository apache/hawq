/*
 * GetApplicationReportResponse.cpp
 *
 *  Created on: Jul 8, 2014
 *      Author: bwang
 */

#include "GetApplicationReportResponse.h"

namespace libyarn {

GetApplicationReportResponse::GetApplicationReportResponse() {
	responseProto = GetApplicationReportResponseProto::default_instance();
}

GetApplicationReportResponse::GetApplicationReportResponse(
		const GetApplicationReportResponseProto &proto) :
		responseProto(proto) {
}

GetApplicationReportResponse::~GetApplicationReportResponse() {
}

GetApplicationReportResponseProto& GetApplicationReportResponse::proto() {
	return responseProto;
}


void GetApplicationReportResponse::setApplicationReport(ApplicationReport &appReport) {
	ApplicationReportProto *proto = new ApplicationReportProto();
	proto->CopyFrom(appReport.getProto());
	responseProto.set_allocated_application_report(proto);
}

ApplicationReport GetApplicationReportResponse::getApplicationReport() {
	return ApplicationReport(responseProto.application_report());
}

} /* namespace libyarn */
