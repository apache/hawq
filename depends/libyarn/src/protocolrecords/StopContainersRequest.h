/*
 * StopContainersRequest.h
 *
 *  Created on: Jul 18, 2014
 *      Author: bwang
 */

#ifndef STOPCONTAINERSREQUEST_H_
#define STOPCONTAINERSREQUEST_H_

#include <list>

#include "YARN_yarn_service_protos.pb.h"
#include "records/ContainerId.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {
/*
message StopContainersRequestProto {
  repeated ContainerIdProto container_id = 1;
}
 */
class StopContainersRequest {
public:
	StopContainersRequest();
	StopContainersRequest(const StopContainersRequestProto &proto);
	virtual ~StopContainersRequest();

	StopContainersRequestProto& getProto();

	void setContainerIds(list<ContainerId> &containerIds);
	list<ContainerId> getContainerIds();

private:
	StopContainersRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* STOPCONTAINERSREQUEST_H_ */
