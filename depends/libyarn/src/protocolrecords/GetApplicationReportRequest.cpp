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

#include "GetApplicationReportRequest.h"

namespace libyarn {

GetApplicationReportRequest::GetApplicationReportRequest() {
	requestProto = GetApplicationReportRequestProto::default_instance();
}

GetApplicationReportRequest::GetApplicationReportRequest(
		const GetApplicationReportRequestProto &proto) :
		requestProto(proto) {
}

GetApplicationReportRequest::~GetApplicationReportRequest() {
}

GetApplicationReportRequestProto& GetApplicationReportRequest::getProto() {
	return requestProto;
}

void GetApplicationReportRequest::setApplicationId(ApplicationId &appId) {
	ApplicationIdProto* proto = new ApplicationIdProto();
	proto->CopyFrom(appId.getProto());
	requestProto.set_allocated_application_id(proto);
}

ApplicationId GetApplicationReportRequest::getApplicationId() {
	return ApplicationId(requestProto.application_id());
}

} /* namespace libyarn */
