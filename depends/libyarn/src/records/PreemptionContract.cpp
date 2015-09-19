/*
 * PreemptionContract.cpp
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#include "PreemptionContract.h"

namespace libyarn {

PreemptionContract::PreemptionContract(){
	contractProto = PreemptionContractProto::default_instance();
}

PreemptionContract::PreemptionContract(const PreemptionContractProto &proto) :contractProto(proto){
}

PreemptionContract::~PreemptionContract() {
}

PreemptionContractProto& PreemptionContract::getProto() {
	return contractProto;
}

set<PreemptionContainer> PreemptionContract::getContainers() {
	set<PreemptionContainer> containerSets;
	for (int i = 0; i < contractProto.container_size(); i++) {
		PreemptionContainerProto proto = contractProto.container(i);
		PreemptionContainer container(proto);
		containerSets.insert(container);
	}
	return containerSets;
}

void PreemptionContract::setContainers(set<PreemptionContainer> &containers) {
	set<PreemptionContainer>::iterator it = containers.begin();
	for (; it != containers.end(); it++) {
		PreemptionContainerProto* containerProto =
				contractProto.add_container();
		containerProto->CopyFrom((*it).getProto());
	}
}

list<PreemptionResourceRequest> PreemptionContract::getResourceRequest() {
	list<PreemptionResourceRequest> requestLists;
	for (int i = 0; i < contractProto.resource_size(); i++) {
		PreemptionResourceRequestProto proto = contractProto.resource(i);
		PreemptionResourceRequest request(proto);
		requestLists.push_back(request);
	}
	return requestLists;
}

void PreemptionContract::setResourceRequest(
		list<PreemptionResourceRequest> &req) {
	list<PreemptionResourceRequest>::iterator it = req.begin();
	for (; it != req.end(); it++) {
		PreemptionResourceRequestProto* requestProto =
				contractProto.add_resource();
		requestProto->CopyFrom((*it).getProto());
	}
}

} /* namespace libyarn */
