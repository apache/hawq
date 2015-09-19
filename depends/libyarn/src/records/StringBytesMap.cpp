/*
 * StringBytesMap.cpp
 *
 *  Created on: Jul 22, 2014
 *      Author: jcao
 */

#include "StringBytesMap.h"

namespace libyarn {

StringBytesMap::StringBytesMap() {
	mapProto = StringBytesMapProto::default_instance();
}

StringBytesMap::StringBytesMap(const StringBytesMapProto &proto) :
		mapProto(proto) {
}

StringBytesMap::~StringBytesMap() {
}

StringBytesMapProto& StringBytesMap::getProto() {
	return mapProto;
}

void StringBytesMap::setKey(string &key) {
	mapProto.set_key(key);
}
string StringBytesMap::getKey() {
	return mapProto.key();
}

void StringBytesMap::setValue(string &value) {
	mapProto.set_value(value);
}

string StringBytesMap::getValue() {
	return mapProto.value();
}


} /* namespace libyarn */
