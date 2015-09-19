/*
 * YarnClusterMetrics.cpp
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#include "YarnClusterMetrics.h"

namespace libyarn {

YarnClusterMetrics::YarnClusterMetrics(const YarnClusterMetricsProto &proto) :
		metricsProto(proto) {
}

YarnClusterMetrics::YarnClusterMetrics() {
	metricsProto = YarnClusterMetricsProto::default_instance();
}

YarnClusterMetrics::~YarnClusterMetrics() {
}

YarnClusterMetricsProto& YarnClusterMetrics::getProto() {
	return metricsProto;
}

void YarnClusterMetrics::setNumNodeManagers(int32_t numNodeManagers) {
	metricsProto.set_num_node_managers(numNodeManagers);
}

int32_t YarnClusterMetrics::getNumNodeManagers() {
	return metricsProto.num_node_managers();
}

} /* namespace libyarn */
