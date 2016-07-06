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

#include "GetApplicationReportResponse.h"

namespace libyarn {

GetApplicationReportResponse::GetApplicationReportResponse() {
	responseProto = GetApplicationReportResponseProto::default_instance();
}

GetApplicationReportResponse::GetApplicationReportResponse(
		const GetApplicationReportResponseProto &proto) :
		responseProto(proto) {
}

GetApplicationReportResponse::~GetApplicationReportResponse() {
}

GetApplicationReportResponseProto& GetApplicationReportResponse::getProto() {
	return responseProto;
}


void GetApplicationReportResponse::setApplicationReport(ApplicationReport &appReport) {
	ApplicationReportProto *proto = new ApplicationReportProto();
	proto->CopyFrom(appReport.getProto());
	responseProto.set_allocated_application_report(proto);
}

ApplicationReport GetApplicationReportResponse::getApplicationReport() {
	return ApplicationReport(responseProto.application_report());
}

} /* namespace libyarn */
