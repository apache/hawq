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

#ifndef PREEMPTIONMESSAGE_H_
#define PREEMPTIONMESSAGE_H_

#include "YARN_yarn_protos.pb.h"
#include "StrictPreemptionContract.h"
#include "PreemptionContract.h"

using namespace hadoop::yarn;

namespace libyarn {

class PreemptionMessage {
public:
	PreemptionMessage();
	PreemptionMessage(const PreemptionMessageProto &proto);
	virtual ~PreemptionMessage();

	PreemptionMessageProto& getProto();

	void setStrictContract(StrictPreemptionContract &strict);
	StrictPreemptionContract getStrictContract();

	void setContract(PreemptionContract& c);
	PreemptionContract getContract();

private:
	PreemptionMessageProto preMsgProto;
};

} /* namespace libyarn */

#endif /* PREEMPTIONMESSAGE_H_ */


//message PreemptionMessageProto {
//  optional StrictPreemptionContractProto strictContract = 1;
//  optional PreemptionContractProto contract = 2;
//}
