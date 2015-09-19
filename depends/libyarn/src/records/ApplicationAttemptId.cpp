/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#include "ApplicationAttemptId.h"

namespace libyarn {

ApplicationAttemptId::ApplicationAttemptId(){
	attemptIdProto = ApplicationAttemptIdProto::default_instance();
}

ApplicationAttemptId::ApplicationAttemptId(const ApplicationAttemptIdProto &proto) :
		attemptIdProto(proto) {
}

ApplicationAttemptId::~ApplicationAttemptId() {
}

ApplicationAttemptIdProto& ApplicationAttemptId::getProto(){
	return attemptIdProto;
}

void ApplicationAttemptId::setApplicationId(ApplicationID &appId) {
	ApplicationIdProto *proto = new ApplicationIdProto();
	proto->CopyFrom(appId.getProto());
	attemptIdProto.set_allocated_application_id(proto);
}

ApplicationID ApplicationAttemptId::getApplicationId() {
	return ApplicationID(attemptIdProto.application_id());
}

void ApplicationAttemptId::setAttemptId(int32_t attemptId) {
	attemptIdProto.set_attemptid(attemptId);
}

int32_t ApplicationAttemptId::getAttemptId() {
	return attemptIdProto.attemptid();
}

} /* namespace libyarn */
