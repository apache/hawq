/*
 * GetContainersResponse.cpp
 *
 *  Created on: Jan 5, 2015
 *      Author: weikui
 */
#include "GetContainersResponse.h"

namespace libyarn {

GetContainersResponse::GetContainersResponse() {
	responseProto = GetContainersResponseProto::default_instance();
}

GetContainersResponse::GetContainersResponse(
		const GetContainersResponseProto &proto) :
		responseProto(proto) {
}

GetContainersResponse::~GetContainersResponse() {
}

GetContainersResponseProto& GetContainersResponse::getProto() {
	return responseProto;
}

list<ContainerReport> GetContainersResponse::getcontainersReportList() {
	list<ContainerReport> reportList;
	for (int i = 0; i < responseProto.containers_reports_size(); i++) {
		ContainerReportProto reportProto = responseProto.containers_reports(i);
		reportList.push_back(ContainerReport(reportProto));
	}
	return reportList;
}

void GetContainersResponse::setcontainersReportList(
		list<ContainerReport> &containersReport) {
	list<ContainerReport>::iterator it = containersReport.begin();
	for (; it != containersReport.end(); it++) {
		ContainerReportProto* reportProto = responseProto.add_containers_reports();
		reportProto->CopyFrom((*it).getProto());
	}
}

} /* namespace libyarn */




