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
