/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
