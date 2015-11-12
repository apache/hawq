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
