/*
 * KillApplicationResponse.cpp
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#include "KillApplicationResponse.h"

namespace libyarn {

KillApplicationResponse::KillApplicationResponse() {
	responseProto = KillApplicationResponseProto::default_instance();
}

KillApplicationResponse::KillApplicationResponse(const KillApplicationResponseProto &proto) :
		responseProto(proto) {
}

KillApplicationResponse::~KillApplicationResponse() {
}

KillApplicationResponseProto& KillApplicationResponse::getProto() {
	return responseProto;
}

} /* namespace libyarn */
