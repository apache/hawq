/*
 * SerializedException.cpp
 *
 *  Created on: Jul 31, 2014
 *      Author: bwang
 */

#include "SerializedException.h"

namespace libyarn {

SerializedException::SerializedException() {
	exceptionProto = SerializedExceptionProto::default_instance();
}

SerializedException::SerializedException(const SerializedExceptionProto &proto) :
		exceptionProto(proto) {
}

SerializedException::~SerializedException() {
}

SerializedExceptionProto& SerializedException::getProto() {
	return exceptionProto;
}

void SerializedException::setMessage(string &message) {
	exceptionProto.set_message(message);

}

string SerializedException::getMessage() {
	return exceptionProto.message();
}

void SerializedException::setTrace(string &trace) {
	exceptionProto.set_trace(trace);
}

string SerializedException::getTrace() {
	return exceptionProto.trace();
}

void SerializedException::setClassName(string &name) {
	exceptionProto.set_class_name(name);
}

string SerializedException::getClassName() {
	return exceptionProto.class_name();
}

void SerializedException::setCause(SerializedException &cause) {
	SerializedExceptionProto* proto = new SerializedExceptionProto();
	proto->CopyFrom(cause.getProto());
	exceptionProto.set_allocated_cause(proto);
}

SerializedException SerializedException::getCause() {
	return SerializedException(exceptionProto.cause());
}

} /* namespace libyarn */
