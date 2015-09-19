/*
 * PreemptionContainer.cpp
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
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
