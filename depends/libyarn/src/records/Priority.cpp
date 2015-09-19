/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#include "Priority.h"

namespace libyarn {

Priority::Priority() {
	priProto = PriorityProto::default_instance();
}

Priority::Priority(int32_t priority) {
	Priority();
	priProto.set_priority(priority);
}

Priority::Priority(const PriorityProto &proto) : priProto (proto) {
}

Priority::~Priority() {
}

PriorityProto& Priority::getProto(){
	return priProto;
}

void Priority::setPriority(int32_t priority){
	priProto.set_priority(priority);
}

int32_t Priority::getPriority(){
	return priProto.priority();
}

} /* namespace libyarn */
