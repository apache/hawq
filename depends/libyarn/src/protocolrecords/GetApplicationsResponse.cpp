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

#include "GetApplicationsResponse.h"

namespace libyarn {

GetApplicationsResponse::GetApplicationsResponse() {
	responseProto = GetApplicationsResponseProto::default_instance();
}

GetApplicationsResponse::GetApplicationsResponse(
		const GetApplicationsResponseProto &proto) :
		responseProto(proto) {
}

GetApplicationsResponse::~GetApplicationsResponse() {
}

GetApplicationsResponseProto& GetApplicationsResponse::getProto() {
	return responseProto;
}

list<ApplicationReport> GetApplicationsResponse::getApplicationList() {
	list<ApplicationReport> reportList;
	for (int i = 0; i < responseProto.applications_size(); i++) {
		ApplicationReportProto reportProto = responseProto.applications(i);
		reportList.push_back(ApplicationReport(reportProto));
	}
	return reportList;
}

void GetApplicationsResponse::setApplicationList(
		list<ApplicationReport> &applications) {
	list<ApplicationReport>::iterator it = applications.begin();
	for (; it != applications.end(); it++) {
		ApplicationReportProto* reportProto = responseProto.add_applications();
		reportProto->CopyFrom((*it).getProto());
	}
}

} /* namespace libyarn */
