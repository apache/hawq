/*
 * PreemptionMessage.cpp
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */

#include "PreemptionMessage.h"

namespace libyarn {

PreemptionMessage::PreemptionMessage() {
	preMsgProto = PreemptionMessageProto::default_instance();
}

PreemptionMessage::PreemptionMessage(const PreemptionMessageProto &proto) :
		preMsgProto(proto) {
}

PreemptionMessage::~PreemptionMessage() {
}

PreemptionMessageProto& PreemptionMessage::getProto() {
	return preMsgProto;
}

void PreemptionMessage::setStrictContract(StrictPreemptionContract &strict) {
	StrictPreemptionContractProto *contractProto =
			new StrictPreemptionContractProto();
	contractProto->CopyFrom(strict.getProto());
	preMsgProto.set_allocated_strictcontract(contractProto);
}

StrictPreemptionContract PreemptionMessage::getStrictContract() {
	return StrictPreemptionContract(preMsgProto.strictcontract());
}

void PreemptionMessage::setContract(PreemptionContract &c) {
	PreemptionContractProto* pcProto = new PreemptionContractProto();
	pcProto->CopyFrom(c.getProto());
	preMsgProto.set_allocated_contract(pcProto);
}

PreemptionContract PreemptionMessage::getContract() {
	return PreemptionContract(preMsgProto.contract());
}

} /* namespace libyarn */
