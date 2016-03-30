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

#include <set>
#include "PreemptionContract.h"

using std::set;

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
