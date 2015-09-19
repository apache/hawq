/*
 * Resource.h
 *
 *  Created on: Jul 10, 2014
 *      Author: bwang
 */

#ifndef RESOURCE_H_
#define RESOURCE_H_

#include "YARN_yarn_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message ResourceProto {
  optional int32 memory = 1;
  optional int32 virtual_cores = 2;
}
 */

class Resource {
public:
	Resource();
	Resource(const ResourceProto &proto);
	virtual ~Resource();

	ResourceProto& getProto();

	void setMemory(int memory);
	int getMemory();

	void setVirtualCores(int virtualCores);
	int getVirtualCores();

private:
	ResourceProto resourceProto;
};


} /* namespace libyarn */

#endif /* RESOURCE_H_ */

