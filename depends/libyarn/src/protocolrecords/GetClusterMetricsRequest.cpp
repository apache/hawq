/*
 * GetClusterMetricsRequest.cpp
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#include "GetClusterMetricsRequest.h"

namespace libyarn {

GetClusterMetricsRequest::GetClusterMetricsRequest() {
	requestProto = GetClusterMetricsRequestProto::default_instance();
}

GetClusterMetricsRequest::GetClusterMetricsRequest(
		const GetClusterMetricsRequestProto &proto) :
		requestProto(proto) {
}

GetClusterMetricsRequest::~GetClusterMetricsRequest() {
}

GetClusterMetricsRequestProto& GetClusterMetricsRequest::getProto() {
	return requestProto;
}

} /* namespace libyarn */
