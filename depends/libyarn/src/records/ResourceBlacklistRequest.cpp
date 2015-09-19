/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
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
