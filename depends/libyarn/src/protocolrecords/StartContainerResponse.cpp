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

#include "StartContainerResponse.h"

namespace libyarn {

StartContainerResponse::StartContainerResponse() {
	responseProto = StartContainerResponseProto::default_instance();
}

StartContainerResponse::StartContainerResponse(const StartContainerResponseProto &proto) : responseProto(proto) {
}

StartContainerResponse::~StartContainerResponse() {
}

StartContainerResponseProto& StartContainerResponse::getProto() {
	return responseProto;
}

void StartContainerResponse::setServicesMetaData(list<StringBytesMap> datas) {
	for (list<StringBytesMap>::iterator it = datas.begin(); it != datas.end(); it++) {
		StringBytesMapProto *proto = responseProto.add_services_meta_data();
		proto->CopyFrom((*it).getProto());
	}
}

list<StringBytesMap> StartContainerResponse::getServicesMetaData() {
	list<StringBytesMap> maps;
	int size = responseProto.services_meta_data_size();
	for (int i = 0; i < size; i++) {
		StringBytesMapProto proto = responseProto.services_meta_data(i);
		StringBytesMap map(proto);
		maps.push_back(map);
	}
	return maps;
}

} /* namespace libyarn */
