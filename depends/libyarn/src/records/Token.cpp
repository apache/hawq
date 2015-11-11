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

#include "Token.h"

namespace libyarn {

Token::Token() {
	tokenProto = TokenProto::default_instance();
}

Token::Token(const TokenProto &proto) : tokenProto(proto) {
}

Token::~Token() {
}

TokenProto& Token::getProto() {
	return tokenProto;
}

void Token::setIdentifier(string &identifier) {
	tokenProto.set_identifier(identifier);
}

string Token::getIdentifier() {
	return tokenProto.identifier();
}

void Token::setPassword(string &passwd) {
	tokenProto.set_password(passwd);
}

string Token::getPassword() {
	return tokenProto.password();
}

void Token::setKind(string &kind) {
	tokenProto.set_kind(kind);
}

string Token::getKind() {
	return tokenProto.kind();
}

void Token::setService(string &service) {
	tokenProto.set_service(service);
}

string Token::getService() {
	return tokenProto.service();
}

} /* namespace libyarn */
