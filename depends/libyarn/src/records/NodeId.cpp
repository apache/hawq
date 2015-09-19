/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#include "NodeId.h"

namespace libyarn {

NodeId::NodeId() {
	nodeIdProto = NodeIdProto::default_instance();
}

NodeId::NodeId(const NodeIdProto &proto) : nodeIdProto(proto) {
}

NodeId::~NodeId() {
}


NodeIdProto& NodeId::getProto() {
	return nodeIdProto;
}

void NodeId::setHost(string &host) {
	nodeIdProto.set_host(host);
}

string NodeId::getHost() {
	return nodeIdProto.host();
}

void NodeId::setPort(int32_t port) {
	nodeIdProto.set_port(port);
}

int32_t NodeId::getPort() {
	return nodeIdProto.port();
}

} /* namespace libyarn */
