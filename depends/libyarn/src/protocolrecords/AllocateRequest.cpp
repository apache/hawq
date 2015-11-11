/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
