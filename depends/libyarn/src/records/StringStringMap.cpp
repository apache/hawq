/*
 * StringStringMap.cpp
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#include "StringStringMap.h"

namespace libyarn {

StringStringMap::StringStringMap() {
	ssMapProto = StringStringMapProto::default_instance();
}

StringStringMap::StringStringMap(const StringStringMapProto &proto) :
		ssMapProto(proto) {
}

StringStringMap::~StringStringMap() {
}

StringStringMapProto& StringStringMap::getProto() {
	return ssMapProto;
}

void StringStringMap::setKey(string &key) {
	ssMapProto.set_key(key);
}

string StringStringMap::getKey() {
	return ssMapProto.key();
}

void StringStringMap::setValue(string &value) {
	ssMapProto.set_value(value);
}

string StringStringMap::getValue() {
	return ssMapProto.value();
}

} /* namespace libyarn */
