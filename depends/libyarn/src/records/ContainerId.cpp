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

#include "ContainerId.h"

namespace libyarn {

ContainerId::ContainerId() {
	containerIdProto = ContainerIdProto::default_instance();
}

ContainerId::ContainerId(const ContainerIdProto &proto) : containerIdProto(proto) {
}

ContainerId::~ContainerId() {
}

ContainerIdProto& ContainerId::getProto(){
	return containerIdProto;
}

void ContainerId::setApplicationId(ApplicationID &appId) {
	ApplicationIdProto *proto = new ApplicationIdProto();
	proto->CopyFrom(appId.getProto());
	containerIdProto.set_allocated_app_id(proto);
}

ApplicationID ContainerId::getApplicationId() {
	return ApplicationID(containerIdProto.app_id());
}

void ContainerId::setApplicationAttemptId(ApplicationAttemptId &appAttemptId) {
	ApplicationAttemptIdProto *proto = new ApplicationAttemptIdProto();
	proto->CopyFrom(appAttemptId.getProto());
	containerIdProto.set_allocated_app_attempt_id(proto);
}

ApplicationAttemptId ContainerId::getApplicationAttemptId() {
	return ApplicationAttemptId(containerIdProto.app_attempt_id());
}

void ContainerId::setId(int32_t id) {
	containerIdProto.set_id(id);
}

int32_t ContainerId::getId() {
	return containerIdProto.id();
}

} /* namespace libyarn */
