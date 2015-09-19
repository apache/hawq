/*
 * SubmitApplicationRequest.cpp
 *
 *  Created on: Jul 14, 2014
 *      Author: bwang
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
