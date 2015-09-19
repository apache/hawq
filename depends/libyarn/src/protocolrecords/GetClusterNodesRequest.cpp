/*
 * GetClusterNodesRequest.cpp
 *
 *  Created on: Jul 18, 2014
 *      Author: bwang
 */

#include "GetClusterNodesRequest.h"

namespace libyarn {

GetClusterNodesRequest::GetClusterNodesRequest() {
	requestProto = GetClusterNodesRequestProto::default_instance();
}

GetClusterNodesRequest::GetClusterNodesRequest(const GetClusterNodesRequestProto &proto) : requestProto(proto) {
}

GetClusterNodesRequest::~GetClusterNodesRequest() {
}

GetClusterNodesRequestProto& GetClusterNodesRequest::getProto() {
	return requestProto;
}

void GetClusterNodesRequest::setNodeStates(list<NodeState> &states) {
	for (list<NodeState>::iterator it = states.begin(); it != states.end(); it++) {
		requestProto.add_nodestates((NodeStateProto)(*it));
	}
}

list<NodeState> GetClusterNodesRequest::getNodeStates() {
	list<NodeState> states;
	int size = requestProto.nodestates_size();
	for (int i = 0; i < size; i++) {
		states.push_back((NodeState)requestProto.nodestates(i));
	}
	return states;
}

} /* namespace libyarn */
