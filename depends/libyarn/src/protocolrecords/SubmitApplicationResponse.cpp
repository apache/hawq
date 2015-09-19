/*
 * SubmitApplicationResponse.cpp
 *
 *  Created on: Jul 14, 2014
 *      Author: bwang
 */

#include "SubmitApplicationResponse.h"

namespace libyarn {

SubmitApplicationResponse::SubmitApplicationResponse() {
	responseProto = SubmitApplicationResponseProto::default_instance();
}

SubmitApplicationResponse::SubmitApplicationResponse(
		const SubmitApplicationResponseProto &proto) :
		responseProto(proto) {
}

SubmitApplicationResponse::~SubmitApplicationResponse() {
}

SubmitApplicationResponseProto& SubmitApplicationResponse::getProto() {
	return responseProto;
}

} /* namespace libyarn */
