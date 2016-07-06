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

#include "ContainerStatus.h"

namespace libyarn {

ContainerStatus::ContainerStatus() {
	statusProto = ContainerStatusProto::default_instance();
}

ContainerStatus::ContainerStatus(const ContainerStatusProto &proto) : statusProto(proto) {
}

ContainerStatus::~ContainerStatus() {
}

ContainerStatusProto& ContainerStatus::getProto() {
	return statusProto;
}

void ContainerStatus::setContainerId(ContainerId &containerId) {
	ContainerIdProto *proto = new ContainerIdProto();
	proto->CopyFrom(containerId.getProto());
	statusProto.set_allocated_container_id(proto);
}

ContainerId ContainerStatus::getContainerId() {
	return ContainerId(statusProto.container_id());
}

void ContainerStatus::setContainerState(ContainerState state) {
	statusProto.set_state((ContainerStateProto)state);
}

ContainerState ContainerStatus::getContainerState() {
	return (ContainerState)statusProto.state();
}

void ContainerStatus::setDiagnostics(string &diagnostics) {
	statusProto.set_diagnostics(diagnostics);
}

string ContainerStatus::getDiagnostics() {
	return statusProto.diagnostics();
}

void ContainerStatus::setExitStatus(int32_t exitStatus) {
	statusProto.set_exit_status(exitStatus);
}

int32_t ContainerStatus::getExitStatus() {
	return statusProto.exit_status();
}

} /* namespace libyarn */
