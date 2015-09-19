/*
 * GetContainerStatusesRequest.cpp
 *
 *  Created on: Aug 1, 2014
 *      Author: bwang
 */

#include "GetContainerStatusesRequest.h"

namespace libyarn {

GetContainerStatusesRequest::GetContainerStatusesRequest() {
	requestProto = GetContainerStatusesRequestProto::default_instance();
}

GetContainerStatusesRequest::GetContainerStatusesRequest(
		const GetContainerStatusesRequestProto &proto) :
		requestProto(proto) {
}

GetContainerStatusesRequest::~GetContainerStatusesRequest() {
}

GetContainerStatusesRequestProto& GetContainerStatusesRequest::getProto() {
	return requestProto;
}

list<ContainerId> GetContainerStatusesRequest::getContainerIds() {
	list<ContainerId> idList;
	for (int i = 0; i < requestProto.container_id_size(); i++) {
		idList.push_back(ContainerId(requestProto.container_id(i)));
	}
	return idList;
}

void GetContainerStatusesRequest::setContainerIds(
		list<ContainerId> &containerIds) {
	list<ContainerId>::iterator it = containerIds.begin();
	for (; it != containerIds.end(); it++) {
		ContainerIdProto* idProto = requestProto.add_container_id();
		idProto->CopyFrom((*it).getProto());
	}
}

} /* namespace libyarn */
