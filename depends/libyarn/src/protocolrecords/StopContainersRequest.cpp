/*
 * StopContainersRequest.cpp
 *
 *  Created on: Jul 18, 2014
 *      Author: bwang
 */

#include "StopContainersRequest.h"

namespace libyarn {

StopContainersRequest::StopContainersRequest() {
	requestProto = StopContainersRequestProto::default_instance();
}

StopContainersRequest::StopContainersRequest(const StopContainersRequestProto &proto) {
	requestProto = proto;
}

StopContainersRequest::~StopContainersRequest() {
}

StopContainersRequestProto& StopContainersRequest::getProto(){
	return requestProto;
}

void StopContainersRequest::setContainerIds(
		std::list<ContainerId> &containerIds) {
	for (list<ContainerId>::iterator it = containerIds.begin(); it != containerIds.end(); it++) {
		ContainerIdProto *proto = requestProto.add_container_id();
		proto->CopyFrom((*it).getProto());
	}
}

list<ContainerId> StopContainersRequest::getContainerIds() {
	list<ContainerId> cids;
	int size = requestProto.container_id_size();
	for (int i = 0; i < size; i++) {
		ContainerIdProto proto = requestProto.container_id(i);
		ContainerId cid(proto);
		cids.push_back(cid);
	}
	return cids;
}



} /* namespace libyarn */
