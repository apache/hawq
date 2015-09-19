/*
 * AllocateRequest.cpp
 *
 *  Created on: Jul 15, 2014
 *      Author: bwang
 */
#include "AllocateRequest.h"

namespace libyarn {

AllocateRequest::AllocateRequest() {
	requestProto = AllocateRequestProto::default_instance();
}

AllocateRequest::AllocateRequest(const AllocateRequestProto &proto) : requestProto(proto) {
}


AllocateRequest::~AllocateRequest() {
}

AllocateRequestProto& AllocateRequest::getProto() {
	return requestProto;
}

void AllocateRequest::addAsk(ResourceRequest &ask) {
	ResourceRequestProto *proto = requestProto.add_ask();
	proto->CopyFrom(ask.getProto());
}

void AllocateRequest::setAsks(list<ResourceRequest> &asks) {
	for (list<ResourceRequest>::iterator it = asks.begin(); it != asks.end(); it++) {
		ResourceRequestProto *proto = requestProto.add_ask();
		proto->CopyFrom((*it).getProto());
	}
}


list<ResourceRequest> AllocateRequest::getAsks() {
	list<ResourceRequest> asks;
	int size = requestProto.ask_size();
	for (int i = 0; i < size; i++) {
		ResourceRequestProto proto = requestProto.ask(i);
		ResourceRequest request(proto);
		asks.push_back(request);
	}
	return asks;
}

void AllocateRequest::addRelease(ContainerId &release) {
	ContainerIdProto *proto = requestProto.add_release();
	proto->CopyFrom(release.getProto());
}

void AllocateRequest::setReleases(list<ContainerId> &releases) {
	for (list<ContainerId>::iterator it = releases.begin(); it != releases.end(); it++) {
		ContainerIdProto *proto = requestProto.add_release();
		proto->CopyFrom((*it).getProto());
	}
}

list<ContainerId> AllocateRequest::getReleases() {
	list<ContainerId> releases;
	int size = requestProto.release_size();
	for (int i = 0; i < size; i++) {
		ContainerIdProto proto = requestProto.release(i);
		ContainerId containerId(proto);
		releases.push_back(containerId);
	}
	return releases;
}

void AllocateRequest::setBlacklistRequest(ResourceBlacklistRequest &request) {
	ResourceBlacklistRequestProto *proto = new ResourceBlacklistRequestProto();
	proto->CopyFrom(request.getProto());
	requestProto.set_allocated_blacklist_request(proto);
}

ResourceBlacklistRequest AllocateRequest::getBlacklistRequest() {
	return ResourceBlacklistRequest(requestProto.blacklist_request());
}

void AllocateRequest::setResponseId(int32_t responseId) {
	requestProto.set_response_id(responseId);
}

int32_t AllocateRequest::getResponseId() {
	return requestProto.response_id();
}

void AllocateRequest::setProgress(float progress) {
	requestProto.set_progress(progress);
}

float AllocateRequest::getProgress() {
	return requestProto.progress();
}

} /* namespace libyarn */
