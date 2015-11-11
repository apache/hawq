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
