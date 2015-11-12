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
