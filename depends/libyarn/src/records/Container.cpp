/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
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
