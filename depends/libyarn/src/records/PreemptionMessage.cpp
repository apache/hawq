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
