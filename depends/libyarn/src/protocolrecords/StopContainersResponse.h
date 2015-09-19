/*
 * StopContainersResponse.h
 *
 *  Created on: Jul 21, 2014
 *      Author: jcao
 */

#ifndef STOPCONTAINERSRESPONSE_H_
#define STOPCONTAINERSRESPONSE_H_

#include "records/ContainerExceptionMap.h"
#include <list>

#include "YARN_yarn_service_protos.pb.h"
#include "records/ContainerId.h"


using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message StopContainersResponseProto {
  repeated ContainerIdProto succeeded_requests = 1;
  repeated ContainerExceptionMapProto failed_requests = 2;  //TODO
}
 */

class StopContainersResponse {
public:
	StopContainersResponse();
	StopContainersResponse(const StopContainersResponseProto &proto);
	virtual ~StopContainersResponse();

	StopContainersResponseProto& getProto();

	void setSucceededRequests(list<ContainerId> requests);
	list<ContainerId> getSucceededRequests();

	void setFailedRequests(list<ContainerExceptionMap> & requests);
	list<ContainerExceptionMap> getFailedRequests();

private:
	StopContainersResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* STOPCONTAINERSRESPONSE_H_ */
