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

#include "ApplicationId.h"

namespace libyarn {

ApplicationId::ApplicationId() {
	appIdProto = ApplicationIdProto::default_instance();
}

ApplicationId::ApplicationId(const ApplicationIdProto &proto) : appIdProto(proto) {
}

ApplicationId::ApplicationId(const ApplicationId &applicationId){
	appIdProto = applicationId.appIdProto;
}

ApplicationId::~ApplicationId() {
}

ApplicationIdProto& ApplicationId::getProto() {
	return appIdProto;
}

void ApplicationId::setId(int32_t id) {
	appIdProto.set_id(id);
}

int ApplicationId::getId() {
	return appIdProto.id();
}

void ApplicationId::setClusterTimestamp(int64_t timestamp) {
	appIdProto.set_cluster_timestamp(timestamp);
}

int64_t ApplicationId::getClusterTimestamp() {
	return appIdProto.cluster_timestamp();
}

} /* namespace libyarn */

