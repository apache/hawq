/*
 * GetClusterMetricsResponse.cpp
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#include "GetClusterMetricsResponse.h"

namespace libyarn {

GetClusterMetricsResponse::GetClusterMetricsResponse() {
	responseProto = GetClusterMetricsResponseProto::default_instance();
}

GetClusterMetricsResponse::GetClusterMetricsResponse(
		const GetClusterMetricsResponseProto &proto) :
		responseProto(proto) {
}

GetClusterMetricsResponse::~GetClusterMetricsResponse() {
}

YarnClusterMetrics GetClusterMetricsResponse::getClusterMetrics() {
	return YarnClusterMetrics(responseProto.cluster_metrics());
}

void GetClusterMetricsResponse::setClusterMetrics(
		YarnClusterMetrics &clusterMetrics) {
	YarnClusterMetricsProto* metricsProto = new YarnClusterMetricsProto();
	metricsProto->CopyFrom(clusterMetrics.getProto());
	responseProto.set_allocated_cluster_metrics(metricsProto);
}

} /* namespace libyarn */
