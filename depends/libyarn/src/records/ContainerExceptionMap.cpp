/*
 * ContainerExceptionMap.cpp
 *
 *  Created on: Jul 31, 2014
 *      Author: bwang
 */

#include "ContainerExceptionMap.h"

namespace libyarn {

ContainerExceptionMap::ContainerExceptionMap() {
	ceProto = ContainerExceptionMapProto::default_instance();
}

ContainerExceptionMap::ContainerExceptionMap(
		const ContainerExceptionMapProto &proto) :
		ceProto(proto) {
}

ContainerExceptionMap::~ContainerExceptionMap() {
}

ContainerExceptionMapProto& ContainerExceptionMap::getProto() {
	return ceProto;
}

void ContainerExceptionMap::setContainerId(ContainerId &cId) {
	ContainerIdProto* idProto = new ContainerIdProto();
	idProto->CopyFrom(cId.getProto());
	ceProto.set_allocated_container_id(idProto);
}

ContainerId ContainerExceptionMap::getContainerId() {
	return ContainerId(ceProto.container_id());
}

void ContainerExceptionMap::setSerializedException(
		SerializedException & exception) {
	SerializedExceptionProto* proto = new SerializedExceptionProto();
	proto->CopyFrom(exception.getProto());
	ceProto.set_allocated_exception(proto);
}

SerializedException ContainerExceptionMap::getSerializedException() {
	return SerializedException(ceProto.exception());
}

} /* namespace libyarn */
