/*
 * StrictPreemptionContract.cpp
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
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
