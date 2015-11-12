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

#include "GetApplicationsRequest.h"

namespace libyarn {

GetApplicationsRequest::GetApplicationsRequest() {
	requestProto = GetApplicationsRequestProto::default_instance();
}

GetApplicationsRequest::GetApplicationsRequest(
		const GetApplicationsRequestProto &proto) :
		requestProto(proto) {
}

GetApplicationsRequest::~GetApplicationsRequest() {
}

GetApplicationsRequestProto& GetApplicationsRequest::getProto() {
	return requestProto;
}

void GetApplicationsRequest::setApplicationTypes(
		list<string> &applicationTypes) {
	list<string>::iterator it = applicationTypes.begin();
	for (; it != applicationTypes.end(); it++) {
		requestProto.add_application_types(*it);
	}
}

list<string> GetApplicationsRequest::getApplicationTypes() {
	list<string> typsesList;
	for (int i = 0; i < requestProto.application_types_size(); i++) {
		typsesList.push_back(requestProto.application_types(i));
	}
	return typsesList;
}

void GetApplicationsRequest::setApplicationStates(
		list<YarnApplicationState> &applicationStates) {
	list<YarnApplicationState>::iterator it = applicationStates.begin();
	for (; it != applicationStates.end(); it++) {
		requestProto.add_application_states((YarnApplicationStateProto) *it);
	}
}

list<YarnApplicationState> GetApplicationsRequest::getApplicationStates() {
	list<YarnApplicationState> stateList;
	for (int i = 0; i < requestProto.application_states_size(); i++) {
		stateList.push_back(
				(YarnApplicationState) requestProto.application_states(i));
	}
	return stateList;
}

} /* namespace libyarn */
