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


#include "StrictPreemptionContract.h"

namespace libyarn {

StrictPreemptionContract::StrictPreemptionContract() {
	contractProto = StrictPreemptionContractProto::default_instance();
}

StrictPreemptionContract::StrictPreemptionContract(
		const StrictPreemptionContractProto &proto) :
		contractProto(proto) {
}

StrictPreemptionContract::~StrictPreemptionContract() {
}

StrictPreemptionContractProto& StrictPreemptionContract::getProto() {
	return contractProto;
}

set<PreemptionContainer> StrictPreemptionContract::getContainers() {
	set<PreemptionContainer> containerSet;
	for (int i = 0; i < contractProto.container_size(); i++) {
		PreemptionContainerProto proto = contractProto.container(i);
		PreemptionContainer container(proto);
		containerSet.insert(container);
	}
	return containerSet;
}

void StrictPreemptionContract::setContainers(
		set<PreemptionContainer> &containers) {
	set<PreemptionContainer>::iterator it = containers.begin();
	for (; it != containers.end(); it++) {
		PreemptionContainerProto* containerProto =
				contractProto.add_container();
		containerProto->CopyFrom((*it).getProto());
	}
}

} /* namespace libyarn */
