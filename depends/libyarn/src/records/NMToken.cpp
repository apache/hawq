/*
 * NMToken.cpp
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */

#include "NMToken.h"

namespace libyarn {

NMToken::NMToken() {
	nmTokenProto = NMTokenProto::default_instance();
}

NMToken::NMToken(const NMTokenProto &proto) : nmTokenProto(proto) {
}

NMToken::~NMToken() {
}

NMTokenProto& NMToken::getProto() {
	return this->nmTokenProto;
}

void NMToken::setNodeId(NodeId &nodeId) {
	NodeIdProto *proto = new NodeIdProto();
	proto->CopyFrom(nodeId.getProto());
	nmTokenProto.set_allocated_nodeid(proto);
}

NodeId NMToken::getNodeId() {
	NodeIdProto proto = nmTokenProto.nodeid();
	return NodeId(proto);
}

void NMToken::setToken(Token token) {
	TokenProto *proto = new TokenProto();
	proto->CopyFrom(token.getProto());
	nmTokenProto.set_allocated_token(proto);
}

Token NMToken::getToken() {
	return Token(nmTokenProto.token());
}

} /* namespace libyarn */
