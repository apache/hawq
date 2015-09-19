/*
 * StartContainerResponse.cpp
 *
 *  Created on: Jul 22, 2014
 *      Author: jcao
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

list<StringBytesMap> StartContainerResponse::getSerivcesMetaData() {
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
