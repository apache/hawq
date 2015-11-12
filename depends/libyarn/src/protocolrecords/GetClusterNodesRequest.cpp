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
