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
