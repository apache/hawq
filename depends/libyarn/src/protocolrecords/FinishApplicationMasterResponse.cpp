/*
 * FinishApplicationMasterResponse.cpp
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#include "FinishApplicationMasterResponse.h"

namespace libyarn {

FinishApplicationMasterResponse::FinishApplicationMasterResponse() {
	responseProto = FinishApplicationMasterResponseProto::default_instance();
}

FinishApplicationMasterResponse::FinishApplicationMasterResponse(
		const FinishApplicationMasterResponseProto &proto) : responseProto(proto) {
}

FinishApplicationMasterResponse::~FinishApplicationMasterResponse() {
}

FinishApplicationMasterResponseProto& FinishApplicationMasterResponse::getProto() {
	return responseProto;
}

void FinishApplicationMasterResponse::setIsUnregistered(bool isUnregistered) {
	responseProto.set_isunregistered(isUnregistered);
}

bool FinishApplicationMasterResponse::getIsUnregistered() {
	return responseProto.isunregistered();
}

} /* namespace libyarn */
