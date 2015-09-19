/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef RESOURCEREQUEST_H_
#define RESOURCEREQUEST_H_

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"

#include "Priority.h"
#include "Resource.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message ResourceRequestProto {
  optional PriorityProto priority = 1;
  optional string resource_name = 2;
  optional ResourceProto capability = 3;
  optional int32 num_containers = 4;
  optional bool relax_locality = 5 [default = true];
}
*/

class ResourceRequest {
public:
	ResourceRequest();
	ResourceRequest(const ResourceRequestProto &proto);
	virtual ~ResourceRequest();

	ResourceRequestProto& getProto();

	void setPriority(Priority &priority);
	Priority getPriority();

	void setResourceName(string &name);
	string getResourceName();

	void setCapability(Resource &capability);
	Resource getCapability();

	void setNumContainers(int32_t num);
	int32_t getNumContainers();

	void setRelaxLocality(bool relaxLocality);
	bool getRelaxLocality();

private:
	ResourceRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* RESOURCEREQUEST_H_ */

