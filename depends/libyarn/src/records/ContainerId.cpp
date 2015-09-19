/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
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
