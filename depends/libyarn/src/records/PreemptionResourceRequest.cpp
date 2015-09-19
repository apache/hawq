/*
 * PreemptionResourceRequest.cpp
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#include "PreemptionResourceRequest.h"

namespace libyarn {

PreemptionResourceRequest::PreemptionResourceRequest() {
	requestProto = PreemptionResourceRequestProto::default_instance();
}

PreemptionResourceRequest::PreemptionResourceRequest(
		const PreemptionResourceRequestProto &proto) :
		requestProto(proto) {
}

PreemptionResourceRequest::~PreemptionResourceRequest() {
}

PreemptionResourceRequestProto& PreemptionResourceRequest::getProto() {
	return requestProto;
}

ResourceRequest PreemptionResourceRequest::getResourceRequest() {
	return ResourceRequest(requestProto.resource());
}

void PreemptionResourceRequest::setResourceRequest(ResourceRequest &rr) {
	ResourceRequestProto* rrProto = new ResourceRequestProto();
	rrProto->CopyFrom(rr.getProto());
	requestProto.set_allocated_resource(rrProto);
}

} /* namespace libyarn */
