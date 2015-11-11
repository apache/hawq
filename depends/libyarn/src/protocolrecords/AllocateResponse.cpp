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

#include "AllocateResponse.h"

namespace libyarn {

AllocateResponse::AllocateResponse() {
	responseProto = AllocateResponseProto::default_instance();
}

AllocateResponse::AllocateResponse(const AllocateResponseProto &proto) :
		responseProto(proto) {
}

AllocateResponse::~AllocateResponse() {
}


AllocateResponseProto& AllocateResponse::getProto(){
	return responseProto;
}

void AllocateResponse::setAMCommand(AMCommand command) {
	responseProto.set_a_m_command((AMCommandProto)command);
}

AMCommand AllocateResponse::getAMCommand() {
	return (AMCommand)responseProto.a_m_command();
}

void AllocateResponse::setResponseId(int32_t responseId) {
	responseProto.set_response_id(responseId);
}

int32_t AllocateResponse::getResponseId() {
	return responseProto.response_id();
}

void AllocateResponse::setAllocatedContainers(list<Container> containers) {
	for(list<Container>::iterator it = containers.begin(); it != containers.end(); it++) {
		ContainerProto *proto = responseProto.add_allocated_containers();
		proto->CopyFrom((*it).getProto());
	}
}

list<Container> AllocateResponse::getAllocatedContainers() {
	list<Container> allocatedContainers;
	int size = responseProto.allocated_containers_size();
	for (int i = 0; i < size; i++) {
		ContainerProto proto = responseProto.allocated_containers(i);
		Container container(proto);
		allocatedContainers.push_back(container);
	}
	return allocatedContainers;
}


void AllocateResponse::setCompletedContainerStatuses(list<ContainerStatus> statuses) {
	for (list<ContainerStatus>::iterator it = statuses.begin(); it != statuses.end(); it++) {
		ContainerStatusProto *proto = responseProto.add_completed_container_statuses();
		proto->CopyFrom((*it).getProto());
	}
}

list<ContainerStatus> AllocateResponse::getCompletedContainersStatuses() {
	list<ContainerStatus> statuses;
	int size = responseProto.completed_container_statuses_size();
	for (int i = 0; i < size; i++) {
		ContainerStatusProto proto = responseProto.completed_container_statuses(i);
		ContainerStatus status(proto);
		statuses.push_back(status);
	}
	return statuses;
}

void AllocateResponse::setResourceLimit(Resource &limit) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(limit.getProto());
	responseProto.set_allocated_limit(proto);
}

Resource AllocateResponse::getResourceLimit() {
	return Resource(responseProto.limit());
}

void AllocateResponse::setUpdatedNodes(list<NodeReport> &updatedNodes) {
	for (list<NodeReport>::iterator it = updatedNodes.begin();
			it != updatedNodes.end(); it++) {
		NodeReportProto* proto = responseProto.add_updated_nodes();
		proto->CopyFrom((*it).getProto());
	}
}

list<NodeReport> AllocateResponse::getUpdatedNodes() {
	list<NodeReport> rList;
	for (int i = 0; i < responseProto.updated_nodes_size(); i++) {
		rList.push_back(NodeReport(responseProto.updated_nodes(i)));
	}
	return rList;
}

void AllocateResponse::setNumClusterNodes(int32_t num) {
	responseProto.set_num_cluster_nodes(num);
}

int32_t AllocateResponse::getNumClusterNodes() {
	return responseProto.num_cluster_nodes();
}

void AllocateResponse::setPreemptionMessage(PreemptionMessage &preempt) {
	PreemptionMessageProto* meProto = new PreemptionMessageProto();
	meProto->CopyFrom(preempt.getProto());
	responseProto.set_allocated_preempt(meProto);
}

PreemptionMessage AllocateResponse::getPreemptionMessage() {
	return PreemptionMessage(responseProto.preempt());
}

void AllocateResponse::setNMTokens(list<NMToken> nmTokens) {
	for (list<NMToken>::iterator it = nmTokens.begin(); it != nmTokens.end(); it++) {
		NMTokenProto *proto = responseProto.add_nm_tokens();
		proto->CopyFrom((*it).getProto());
	}
}

list<NMToken> AllocateResponse::getNMTokens() {
	list<NMToken> nmTokens;
	int size = responseProto.nm_tokens_size();
	for (int i = 0; i < size; i++) {
		NMTokenProto proto = responseProto.nm_tokens(i);
		NMToken token(proto);
		nmTokens.push_back(token);
	}
	return nmTokens;
}

} /* namespace libyarn */
