/*
 * StringLocalResourceMap.cpp
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#include "StringLocalResourceMap.h"

namespace libyarn {

StringLocalResourceMap::StringLocalResourceMap() {
	localResourceProto = StringLocalResourceMapProto::default_instance();
}

StringLocalResourceMap::StringLocalResourceMap(
		const StringLocalResourceMapProto &proto) :
		localResourceProto(proto) {
}

StringLocalResourceMap::~StringLocalResourceMap() {
}

StringLocalResourceMapProto& StringLocalResourceMap::getProto() {
	return localResourceProto;
}

void StringLocalResourceMap::setKey(string &key) {
	localResourceProto.set_key(key);
}

string StringLocalResourceMap::getKey() {
	return localResourceProto.key();
}

void StringLocalResourceMap::setLocalResource(LocalResource &resource) {
	LocalResourceProto* resourceProto = new LocalResourceProto();
	resourceProto->CopyFrom(resource.getProto());
	localResourceProto.set_allocated_value(resourceProto);
}

LocalResource StringLocalResourceMap::getLocalResource() {
	return LocalResource(localResourceProto.value());
}

} /* namespace libyarn */
