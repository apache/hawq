/*
 * GetClusterNodesResponse.cpp
 *
 *  Created on: Jul 18, 2014
 *      Author: bwang
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
		GetClusterNodesResponseProto *proto = new GetClusterNodesResponseProto();
		proto->CopyFrom((*it).getProto());
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
