/*
 * StartContainersResponse.h
 *
 *  Created on: Jul 20, 2014
 *      Author: jcao
 */

#ifndef STARTCONTAINERSRESPONSE_H_
#define STARTCONTAINERSRESPONSE_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/StringBytesMap.h"
#include "records/ContainerExceptionMap.h"
#include "records/ContainerId.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {
/*
message StartContainersResponseProto {
  repeated StringBytesMapProto services_meta_data = 1;
  repeated ContainerIdProto succeeded_requests = 2;
  repeated ContainerExceptionMapProto failed_requests = 3;
}
 */
class StartContainersResponse {
public:
	StartContainersResponse();
	StartContainersResponse(const StartContainersResponseProto &proto);
	virtual ~StartContainersResponse();

	StartContainersResponseProto& getProto();

	void setServicesMetaData(list<StringBytesMap> &maps);
	list<StringBytesMap> getServicesMetaData();

	void setSucceededRequests(list<ContainerId> &requests);
	list<ContainerId> getSucceededRequests();

	void setFailedRequests(list<ContainerExceptionMap> & requests);
	list<ContainerExceptionMap> getFailedRequests();

private:
	StartContainersResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* STARTCONTAINERSRESPONSE_H_ */
