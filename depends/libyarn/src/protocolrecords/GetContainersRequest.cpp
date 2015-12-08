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

#include "GetContainersRequest.h"

namespace libyarn {

GetContainersRequest::GetContainersRequest() {
	requestProto = GetContainersRequestProto::default_instance();
}

GetContainersRequest::GetContainersRequest(
		const GetContainersRequestProto &proto) :
		requestProto(proto) {
}

GetContainersRequest::~GetContainersRequest() {
}

GetContainersRequestProto& GetContainersRequest::getProto() {
	return requestProto;
}

void GetContainersRequest::setApplicationAttemptId(ApplicationAttemptId &appAttemptId) {
	ApplicationAttemptIdProto *proto = new ApplicationAttemptIdProto();
	proto->CopyFrom(appAttemptId.getProto());
	requestProto.set_allocated_application_attempt_id(proto);
}

ApplicationAttemptId GetContainersRequest::getApplicationAttemptId() {
	return ApplicationAttemptId(requestProto.application_attempt_id());
}

} /* namespace libyarn */





