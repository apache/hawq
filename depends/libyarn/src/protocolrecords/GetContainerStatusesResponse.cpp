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

#include "GetContainerStatusesResponse.h"
using std::list;

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
