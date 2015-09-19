/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef PRIORITY_H_
#define PRIORITY_H_

#include "YARN_yarn_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message PriorityProto {
  optional int32 priority = 1;
}
 */

class Priority {
public:
	Priority();
	Priority(int32_t priority);
	Priority(const PriorityProto &proto);
	virtual ~Priority();

	PriorityProto& getProto();

	void setPriority(int32_t priority);

	int32_t getPriority();

private:
	PriorityProto priProto;
};

} /* namespace libyarn */

#endif /* PRIORITY_H_ */
