/*
 * GetQueueUserAclsInfoRequest.cpp
 *
 *  Created on: Jul 29, 2014
 *      Author: bwang
 */

#include "GetQueueUserAclsInfoRequest.h"

namespace libyarn {

GetQueueUserAclsInfoRequest::GetQueueUserAclsInfoRequest() {
	requestProto = GetQueueUserAclsInfoRequestProto::default_instance();
}

GetQueueUserAclsInfoRequest::GetQueueUserAclsInfoRequest(
		const GetQueueUserAclsInfoRequestProto &proto) :
		requestProto(proto) {
}

GetQueueUserAclsInfoRequest::~GetQueueUserAclsInfoRequest() {
}

GetQueueUserAclsInfoRequestProto& GetQueueUserAclsInfoRequest::getProto() {
	return requestProto;
}

} /* namespace libyarn */
