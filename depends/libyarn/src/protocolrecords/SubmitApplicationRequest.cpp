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

#include <iostream>

#include "SubmitApplicationRequest.h"

namespace libyarn {

SubmitApplicationRequest::SubmitApplicationRequest() {
	requestProto = SubmitApplicationRequestProto::default_instance();
}

SubmitApplicationRequest::SubmitApplicationRequest(
		const SubmitApplicationRequestProto &proto) :
		requestProto(proto) {
}


SubmitApplicationRequest::~SubmitApplicationRequest() {
}

SubmitApplicationRequestProto& SubmitApplicationRequest::getProto() {
	return requestProto;
}

void SubmitApplicationRequest::setApplicationSubmissionContext(ApplicationSubmissionContext &appCtx) {
	ApplicationSubmissionContextProto* proto = new ApplicationSubmissionContextProto();
	proto->CopyFrom(appCtx.getProto());
	requestProto.set_allocated_application_submission_context(proto);
}

ApplicationSubmissionContext SubmitApplicationRequest::getApplicationSubmissionContext() {
	return ApplicationSubmissionContext(requestProto.application_submission_context());
}

} /* namespace libyarn */
