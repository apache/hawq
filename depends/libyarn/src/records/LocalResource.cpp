/*
 * LocalResource.cpp
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#include "LocalResource.h"

namespace libyarn {

LocalResource::LocalResource() {
	localReProto = LocalResourceProto::default_instance();
}

LocalResource::LocalResource(const LocalResourceProto &proto) :
		localReProto(proto) {
}

LocalResource::~LocalResource() {
}

LocalResourceProto& LocalResource::getProto() {
	return localReProto;
}

URL LocalResource::getResource() {
	return URL(localReProto.resource());
}

void LocalResource::setResource(URL &resource) {
	URLProto* urlProto = new URLProto();
	urlProto->CopyFrom(resource.getProto());
	localReProto.set_allocated_resource(urlProto);
}

long LocalResource::getSize() {
	return localReProto.size();
}

void LocalResource::setSize(long size) {
	localReProto.set_size(size);
}

long LocalResource::getTimestamp() {
	return localReProto.timestamp();
}

void LocalResource::setTimestamp(long timestamp) {
	localReProto.set_timestamp(timestamp);
}

LocalResourceType LocalResource::getType() {
	return (LocalResourceType) localReProto.type();
}

void LocalResource::setType(LocalResourceType &type) {
	localReProto.set_type((LocalResourceTypeProto) type);
}

LocalResourceVisibility LocalResource::getVisibility() {
	return (LocalResourceVisibility) localReProto.visibility();
}

void LocalResource::setVisibility(LocalResourceVisibility &visibility) {
	localReProto.set_visibility((LocalResourceVisibilityProto) visibility);
}

string LocalResource::getPattern() {
	return localReProto.pattern();
}

void LocalResource::setPattern(string &pattern) {
	localReProto.set_pattern(pattern);
}

} /* namespace libyarn */
