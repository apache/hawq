/*
 * StrictPreemptionContract.h
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#ifndef STRICTPREEMPTIONCONTRACT_H_
#define STRICTPREEMPTIONCONTRACT_H_

#include <set>

#include "YARN_yarn_protos.pb.h"

#include "PreemptionContainer.h"

using namespace std;
using namespace hadoop;
using namespace yarn;
namespace libyarn {

class StrictPreemptionContract {
public:
	StrictPreemptionContract(const StrictPreemptionContractProto &proto);
	StrictPreemptionContract();
	virtual ~StrictPreemptionContract();

	StrictPreemptionContractProto& getProto();

	void setContainers(set<PreemptionContainer> &containerSets);
	set<PreemptionContainer> getContainers();

private:
	StrictPreemptionContractProto contractProto;
};

} /* namespace libyarn */

#endif /* STRICTPREEMPTIONCONTRACT_H_ */

//message StrictPreemptionContractProto {
//  repeated PreemptionContainerProto container = 1;
//}
