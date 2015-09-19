/*
 * PreemptionContract.h
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#ifndef PREEMPTIONCONTRACT_H_
#define PREEMPTIONCONTRACT_H_

#include <set>
#include <list>

#include "YARN_yarn_protos.pb.h"

#include "PreemptionResourceRequest.h"
#include "PreemptionContainer.h"

using namespace std;
using namespace hadoop;
using namespace yarn;
namespace libyarn {

class PreemptionContract {
public:
	PreemptionContract();
	PreemptionContract(const PreemptionContractProto &proto);
	virtual ~PreemptionContract();

	PreemptionContractProto& getProto();
	set<PreemptionContainer> getContainers();
	void setContainers(set<PreemptionContainer> &containers);
	list<PreemptionResourceRequest> getResourceRequest();
	void setResourceRequest(list<PreemptionResourceRequest> &req);
private:
	PreemptionContractProto contractProto;
};

} /* namespace libyarn */

#endif /* PREEMPTIONCONTRACT_H_ */

//message PreemptionContractProto {
//  repeated PreemptionResourceRequestProto resource = 1;
//  repeated PreemptionContainerProto container = 2;
//}
