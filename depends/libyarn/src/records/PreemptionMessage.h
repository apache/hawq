/*
 * PreemptionMessage.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
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
