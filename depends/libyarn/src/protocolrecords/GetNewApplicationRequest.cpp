/*
 * GetNewApplicationRequest.cpp
 *
 *  Created on: Jul 10, 2014
 *      Author: bwang
 */

#include "GetNewApplicationRequest.h"

namespace libyarn {

GetNewApplicationRequest::GetNewApplicationRequest() {
	requestProto = GetNewApplicationRequestProto::default_instance();
}

GetNewApplicationRequest::GetNewApplicationRequest(
		const GetNewApplicationRequestProto &proto) :
		requestProto(proto) {
}

GetNewApplicationRequest::~GetNewApplicationRequest() {
}

GetNewApplicationRequestProto& GetNewApplicationRequest::getProto(){
	return requestProto;
}

} /* namespace libyarn */
