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
