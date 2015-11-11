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

#include "FinishApplicationMasterRequest.h"

namespace libyarn {

FinishApplicationMasterRequest::FinishApplicationMasterRequest() {
	requestProto = FinishApplicationMasterRequestProto::default_instance();
}

FinishApplicationMasterRequest::FinishApplicationMasterRequest(
		const FinishApplicationMasterRequestProto &proto) : requestProto(proto) {
}

FinishApplicationMasterRequest::~FinishApplicationMasterRequest() {
}

FinishApplicationMasterRequestProto& FinishApplicationMasterRequest::getProto() {
	return requestProto;
}

void FinishApplicationMasterRequest::setDiagnostics(string &diagnostics) {
	requestProto.set_diagnostics(diagnostics);
}

string FinishApplicationMasterRequest::getDiagnostics() {
	return requestProto.diagnostics();
}

void FinishApplicationMasterRequest::setTrackingUrl(string &url) {
	requestProto.set_tracking_url(url);
}

string FinishApplicationMasterRequest::getTrackingUrl() {
	return requestProto.tracking_url();
}

void FinishApplicationMasterRequest::setFinalApplicationStatus(
		FinalApplicationStatus finalState) {
	requestProto.set_final_application_status(
			(FinalApplicationStatusProto) finalState);
}

FinalApplicationStatus FinishApplicationMasterRequest::getFinalApplicationStatus() {
	return (FinalApplicationStatus) requestProto.final_application_status();
}

} /* namespace libyarn */
