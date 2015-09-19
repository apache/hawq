/*
 * GetQueueInfoRequest.cpp
 *
 *  Created on: Jul 22, 2014
 *      Author: bwang
 */

#include "GetQueueInfoRequest.h"

namespace libyarn {

GetQueueInfoRequest::GetQueueInfoRequest() {
	requestProto = GetQueueInfoRequestProto::default_instance();
}

GetQueueInfoRequest::GetQueueInfoRequest(const GetQueueInfoRequestProto &proto) :
		requestProto(proto) {
}

GetQueueInfoRequest::~GetQueueInfoRequest() {
}

GetQueueInfoRequestProto& GetQueueInfoRequest::getProto() {
	return requestProto;
}

void GetQueueInfoRequest::setQueueName(string &name) {
	requestProto.set_queuename(name);
}

string GetQueueInfoRequest::getQueueName() {
	return requestProto.queuename();
}

void GetQueueInfoRequest::setIncludeApplications(bool includeApplications) {
	requestProto.set_includeapplications(includeApplications);
}

bool GetQueueInfoRequest::getIncludeApplications() {
	return requestProto.includeapplications();
}

void GetQueueInfoRequest::setIncludeChildQueues(bool includeChildQueues) {
	requestProto.set_includechildqueues(includeChildQueues);
}

bool GetQueueInfoRequest::getIncludeChildQueues() {
	return requestProto.includechildqueues();
}

void GetQueueInfoRequest::setRecursive(bool recursive) {
	requestProto.set_recursive(recursive);
}

bool GetQueueInfoRequest::getRecursive() {
	return requestProto.recursive();
}

} /* namespace libyarn */
