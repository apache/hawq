/*
 * PreemptionContainer.h
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#ifndef PREEMPTIONCONTAINER_H_
#define PREEMPTIONCONTAINER_H_

#include "YARN_yarn_protos.pb.h"
#include "ContainerId.h"

using namespace hadoop;
using namespace yarn;
namespace libyarn {

class PreemptionContainer {
public:
	PreemptionContainer();
	PreemptionContainer(const PreemptionContainerProto &proto);
	virtual ~PreemptionContainer();

	PreemptionContainerProto& getProto();
	const PreemptionContainerProto& getProto() const;
	ContainerId getId();
	void setId(ContainerId &id);
	bool operator < (const PreemptionContainer& container) const;
private:
	PreemptionContainerProto pcProto;
};

} /* namespace libyarn */

#endif /* PREEMPTIONCONTAINER_H_ */

//message PreemptionContainerProto {
//  optional ContainerIdProto id = 1;
//}
