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

#include "ResourceBlacklistRequest.h"

namespace libyarn {

ResourceBlacklistRequest::ResourceBlacklistRequest() {
	requestProto = ResourceBlacklistRequestProto::default_instance();
}

ResourceBlacklistRequest::ResourceBlacklistRequest(
		const ResourceBlacklistRequestProto &proto) :
		requestProto(proto) {
}

ResourceBlacklistRequest::~ResourceBlacklistRequest() {
}

ResourceBlacklistRequestProto& ResourceBlacklistRequest::getProto() {
	return requestProto;
}

void ResourceBlacklistRequest::setBlacklistAdditions(list<string> &additions) {
	for (list<string>::iterator it = additions.begin(); it != additions.end(); it++) {
		requestProto.add_blacklist_additions(*it);
	}
}


void ResourceBlacklistRequest::addBlacklistAddition(string &addition) {
	requestProto.add_blacklist_additions(addition);
}

list<string> ResourceBlacklistRequest::getBlacklistAdditions() {
	list<string> additions;
	int size = requestProto.blacklist_additions_size();
	for (int i = 0; i < size; i++) {
		string addition = requestProto.blacklist_additions(i);
		additions.push_back(addition);
	}
	return additions;
}

void ResourceBlacklistRequest::setBlacklistRemovals(list<string> &removals) {
	for (list<string>::iterator it = removals.begin(); it != removals.end(); it++) {
		requestProto.add_blacklist_removals(*it);
	}
}

void ResourceBlacklistRequest::addBlacklistRemoval(string &removal) {
	requestProto.add_blacklist_removals(removal);
}

list<string> ResourceBlacklistRequest::getBlacklistRemovals() {
	list<string> removals;
	int size = requestProto.blacklist_removals_size();
	for (int i = 0; i < size; i++) {
		string removal = requestProto.blacklist_removals(i);
		removals.push_back(removal);
	}
	return removals;
}

} /* namespace libyarn */
