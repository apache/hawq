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

#include "Container.h"

namespace libyarn {

Container::Container() {
	containerProto = ContainerProto::default_instance();
}

Container::Container(const ContainerProto &proto) : containerProto(proto) {
}

Container::~Container() {
}

ContainerProto& Container::getProto() {
	return containerProto;
}

void Container::setId(ContainerId &id) {
	ContainerIdProto *proto = new ContainerIdProto();
	proto->CopyFrom(id.getProto());
	containerProto.set_allocated_id(proto);
}

ContainerId Container::getId() {
	return ContainerId(containerProto.id());
}

void Container::setNodeId(NodeId &nodeId) {
	NodeIdProto *proto = new NodeIdProto();
	proto->CopyFrom(nodeId.getProto());
	containerProto.set_allocated_nodeid(proto);
}

NodeId Container::getNodeId() {
	return NodeId(containerProto.nodeid());
}

void Container::setNodeHttpAddress(string &httpAddress) {
	containerProto.set_node_http_address(httpAddress);
}

string Container::getNodeHttpAddress() {
	return containerProto.node_http_address();
}

void Container::setResource(Resource &resource) {
	ResourceProto *proto = new ResourceProto();
	proto->CopyFrom(resource.getProto());
	containerProto.set_allocated_resource(proto);
}

Resource Container::getResource() {
	return Resource(containerProto.resource());
}

void Container::setPriority(Priority &priority) {
	PriorityProto *proto = new PriorityProto();
	proto->CopyFrom(priority.getProto());
	containerProto.set_allocated_priority(proto);
}

Priority Container::getPriority() {
	return Priority(containerProto.priority());
}

void Container::setContainerToken(Token containerToken) {
	TokenProto *proto = new TokenProto();
	proto->CopyFrom(containerToken.getProto());
	containerProto.set_allocated_container_token(proto);
}

Token Container::getContainerToken() {
	return Token(containerProto.container_token());
}

} /* namespace libyarn */
