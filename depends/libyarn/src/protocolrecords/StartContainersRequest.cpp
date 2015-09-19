/*
 * StartContainersRequest.cpp
 *
 *  Created on: Jul 17, 2014
 *      Author: bwang
 */

#include "StartContainersRequest.h"

namespace libyarn {

StartContainersRequest::StartContainersRequest() {
	requestProto = StartContainersRequestProto::default_instance();
}

StartContainersRequest::StartContainersRequest(
		const StartContainersRequestProto proto) :
		requestProto(proto) {
}

StartContainersRequest::~StartContainersRequest() {
}

StartContainersRequestProto& StartContainersRequest::getProto() {
	return requestProto;
}

void StartContainersRequest::setStartContainerRequests(list<StartContainerRequest> &requests) {
	for (list<StartContainerRequest>::iterator it = requests.begin(); it != requests.end(); it++) {
		StartContainerRequestProto *proto = requestProto.add_start_container_request();
		proto->CopyFrom((*it).getProto());
	}
}

list<StartContainerRequest> StartContainersRequest::getStartContainerRequests() {
	list<StartContainerRequest> requests;
	int size = requestProto.start_container_request_size();
	for (int i = 0; i < size; i++) {
		StartContainerRequestProto proto = requestProto.start_container_request(i);
		StartContainerRequest request(proto);
		requests.push_back(request);
	}
	return requests;
}

} /* namespace libyarn */
