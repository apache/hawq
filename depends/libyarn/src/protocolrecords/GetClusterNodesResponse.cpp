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

#include "GetClusterNodesResponse.h"

namespace libyarn {

GetClusterNodesResponse::GetClusterNodesResponse() {
	responseProto = GetClusterNodesResponseProto::default_instance();
}

GetClusterNodesResponse::GetClusterNodesResponse(const GetClusterNodesResponseProto &proto) : responseProto(proto) {
}

GetClusterNodesResponse::~GetClusterNodesResponse() {
}

GetClusterNodesResponseProto& GetClusterNodesResponse::getProto() {
	return responseProto;
}

void GetClusterNodesResponse::setNodeReports(list<NodeReport> reports) {
	for (list<NodeReport>::iterator it = reports.begin(); it != reports.end(); it++) {
		NodeReportProto *reportProto = responseProto.add_nodereports();
		reportProto->CopyFrom((*it).getProto());
	}
}

list<NodeReport> GetClusterNodesResponse::getNodeReports() {
	list<NodeReport> reports;
	int size = responseProto.nodereports_size();
	for (int i = 0; i < size; i++) {
		NodeReportProto proto = responseProto.nodereports(i);
		NodeReport report(proto);
		reports.push_back(report);
	}
	return reports;
}

} /* namespace libyarn */
