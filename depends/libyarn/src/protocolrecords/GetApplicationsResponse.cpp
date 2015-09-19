/*
 * GetApplicationsResponse.cpp
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#include "GetApplicationsResponse.h"

namespace libyarn {

GetApplicationsResponse::GetApplicationsResponse() {
	responseProto = GetApplicationsResponseProto::default_instance();
}

GetApplicationsResponse::GetApplicationsResponse(
		const GetApplicationsResponseProto &proto) :
		responseProto(proto) {
}

GetApplicationsResponse::~GetApplicationsResponse() {
}

GetApplicationsResponseProto& GetApplicationsResponse::getProto() {
	return responseProto;
}

list<ApplicationReport> GetApplicationsResponse::getApplicationList() {
	list<ApplicationReport> reportList;
	for (int i = 0; i < responseProto.applications_size(); i++) {
		ApplicationReportProto reportProto = responseProto.applications(i);
		reportList.push_back(ApplicationReport(reportProto));
	}
	return reportList;
}

void GetApplicationsResponse::setApplicationList(
		list<ApplicationReport> &applications) {
	list<ApplicationReport>::iterator it = applications.begin();
	for (; it != applications.end(); it++) {
		ApplicationReportProto* reportProto = responseProto.add_applications();
		reportProto->CopyFrom((*it).getProto());
	}
}

} /* namespace libyarn */
