/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
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
