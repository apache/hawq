/*
 * GetContainerStatusesResponse.cpp
 *
 *  Created on: Aug 1, 2014
 *      Author: bwang
 */

#include "GetContainerStatusesResponse.h"

namespace libyarn {

GetContainerStatusesResponse::GetContainerStatusesResponse() {
	responseProto = GetContainerStatusesResponseProto::default_instance();
}

GetContainerStatusesResponse::GetContainerStatusesResponse(
		const GetContainerStatusesResponseProto &proto) :
		responseProto(proto) {
}

GetContainerStatusesResponse::~GetContainerStatusesResponse() {
}

GetContainerStatusesResponseProto& GetContainerStatusesResponse::getProto() {
	return responseProto;
}

list<ContainerStatus> GetContainerStatusesResponse::getContainerStatuses() {
	list<ContainerStatus> statusList;
	for (int i = 0; i < responseProto.status_size(); i++) {
		statusList.push_back(ContainerStatus(responseProto.status(i)));
	}
	return statusList;
}

void GetContainerStatusesResponse::setContainerStatuses(
		list<ContainerStatus> &statuses) {
	list<ContainerStatus>::iterator it = statuses.begin();
	for (; it != statuses.end(); it++) {
		ContainerStatusProto* statusProto = responseProto.add_status();
		statusProto->CopyFrom((*it).getProto());
	}
}

list<ContainerExceptionMap> GetContainerStatusesResponse::getFailedRequests() {
	list<ContainerExceptionMap> mapList;
	for (int i = 0; i < responseProto.failed_requests_size(); i++) {
		mapList.push_back(
				ContainerExceptionMap(responseProto.failed_requests(i)));
	}
	return mapList;
}

void GetContainerStatusesResponse::setFailedRequests(
		list<ContainerExceptionMap> &mapList) {
	list<ContainerExceptionMap>::iterator it = mapList.begin();
	for (; it != mapList.end(); it++) {
		ContainerExceptionMapProto* mapProto =
				responseProto.add_failed_requests();
		mapProto->CopyFrom((*it).getProto());
	}
}

} /* namespace libyarn */
