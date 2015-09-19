/*
 * URL.cpp
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#include "URL.h"

namespace libyarn {

URL::URL() {
	urlProto = URLProto::default_instance();
}

URL::URL(const URLProto &proto) :
		urlProto(proto) {
}

URL::~URL() {
}

URLProto& URL::getProto() {
	return urlProto;
}

string URL::getScheme() {
	return urlProto.scheme();
}

void URL::setScheme(string &scheme) {
	urlProto.set_scheme(scheme);
}

string URL::getHost() {
	return urlProto.host();
}

void URL::setHost(string &host) {
	urlProto.set_host(host);
}

int32_t URL::getPort() {
	return urlProto.port();
}

void URL::setPort(int32_t port) {
	urlProto.set_port(port);
}

string URL::getFile() {
	return urlProto.file();
}

void URL::setFile(string &file) {
	urlProto.set_file(file);
}

string URL::getUserInfo() {
	return urlProto.userinfo();
}

void URL::setUserInfo(string &userInfo) {
	urlProto.set_userinfo(userInfo);
}

} /* namespace libyarn */
