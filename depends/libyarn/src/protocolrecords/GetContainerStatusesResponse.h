/*
 * GetContainerStatusesResponse.h
 *
 *  Created on: Aug 1, 2014
 *      Author: bwang
 */

#ifndef GETCONTAINERSTATUSESRESPONSE_H_
#define GETCONTAINERSTATUSESRESPONSE_H_

#include "records/ContainerExceptionMap.h"
#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/ContainerStatus.h"

using namespace hadoop::yarn;
namespace libyarn {

//message GetContainerStatusesResponseProto {
//  repeated ContainerStatusProto status = 1;
//  repeated ContainerExceptionMapProto failed_requests = 2;
//}

class GetContainerStatusesResponse {
public:
	GetContainerStatusesResponse();
	GetContainerStatusesResponse(const GetContainerStatusesResponseProto &proto);
	virtual ~GetContainerStatusesResponse();

	GetContainerStatusesResponseProto& getProto();

	list<ContainerStatus> getContainerStatuses();
	void setContainerStatuses(list<ContainerStatus> &statuses);

	list<ContainerExceptionMap> getFailedRequests();
	void setFailedRequests(list<ContainerExceptionMap> &mapList);

private:
	GetContainerStatusesResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETCONTAINERSTATUSESRESPONSE_H_ */
