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

#include "StartContainersResponse.h"

namespace libyarn {

StartContainersResponse::StartContainersResponse() {
	responseProto = StartContainersResponseProto::default_instance();
}

StartContainersResponse::StartContainersResponse(
		const StartContainersResponseProto &proto) :
		responseProto(proto) {
}

StartContainersResponse::~StartContainersResponse() {
}

StartContainersResponseProto& StartContainersResponse::getProto() {
	return responseProto;
}

void StartContainersResponse::setServicesMetaData(list<StringBytesMap> &maps) {
	for (list<StringBytesMap>::iterator it = maps.begin(); it != maps.end();
			it++) {
		StringBytesMapProto *proto = responseProto.add_services_meta_data();
		proto->CopyFrom((*it).getProto());
	}
}

list<StringBytesMap> StartContainersResponse::getServicesMetaData() {
	list<StringBytesMap> maps;
	int size = responseProto.services_meta_data_size();
	for (int i = 0; i < size; i++) {
		StringBytesMapProto proto = responseProto.services_meta_data(i);
		StringBytesMap map(proto);
		maps.push_back(map);
	}
	return maps;
}

void StartContainersResponse::setSucceededRequests(
		list<ContainerId> &requests) {
	for (list<ContainerId>::iterator it = requests.begin();
			it != requests.end(); it++) {
		ContainerIdProto *proto = responseProto.add_succeeded_requests();
		proto->CopyFrom((*it).getProto());
	}
}

list<ContainerId> StartContainersResponse::getSucceededRequests() {
	list<ContainerId> ids;
	int size = responseProto.succeeded_requests_size();
	for (int i = 0; i < size; i++) {
		ContainerIdProto proto = responseProto.succeeded_requests(i);
		ids.push_back(ContainerId(proto));
	}
	return ids;
}

void StartContainersResponse::setFailedRequests(
		list<ContainerExceptionMap> & requests) {
	for (list<ContainerExceptionMap>::iterator it = requests.begin();
			it != requests.end(); it++) {
		ContainerExceptionMapProto *proto = responseProto.add_failed_requests();
		proto->CopyFrom((*it).getProto());
	}
}

list<ContainerExceptionMap> StartContainersResponse::getFailedRequests() {
	list<ContainerExceptionMap> ces;
	int size = responseProto.failed_requests_size();
	for (int i = 0; i < size; i++) {
		ContainerExceptionMapProto proto = responseProto.failed_requests(i);
		ces.push_back(ContainerExceptionMap(proto));
	}
	return ces;
}

} /* namespace libyarn */
