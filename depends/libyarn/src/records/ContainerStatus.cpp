/*
 * ContainerStatus.cpp
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
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

void ContainerStatus::setContaierState(ContainerState state) {
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
