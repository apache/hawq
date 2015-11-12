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

#include "PreemptionContainer.h"

namespace libyarn {

PreemptionContainer::PreemptionContainer() {
	pcProto = PreemptionContainerProto::default_instance();
}

PreemptionContainer::PreemptionContainer(const PreemptionContainerProto &proto) :
		pcProto(proto) {
}

PreemptionContainer::~PreemptionContainer() {
}

PreemptionContainerProto& PreemptionContainer::getProto() {
	return pcProto;
}

const PreemptionContainerProto& PreemptionContainer::getProto() const {
	return pcProto;
}

ContainerId PreemptionContainer::getId() {
	return ContainerId(pcProto.id());
}

void PreemptionContainer::setId(ContainerId &id) {
	ContainerIdProto* idProto = new ContainerIdProto();
	idProto->CopyFrom(id.getProto());
	pcProto.set_allocated_id(idProto);
}

bool PreemptionContainer::operator <(
		const PreemptionContainer& container) const {
	PreemptionContainer pContainer = container;
	if (pcProto.id().id() < pContainer.getId().getApplicationId().getId()) {
		return true;
	} else
		return false;
}

} /* namespace libyarn */
