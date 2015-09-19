/*
 * FinishApplicationMasterRequest.cpp
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
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
