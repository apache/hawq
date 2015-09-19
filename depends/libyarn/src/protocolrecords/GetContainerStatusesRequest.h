/*
 * GetContainerStatusesRequest.h
 *
 *  Created on: Aug 1, 2014
 *      Author: bwang
 */

#ifndef GETCONTAINERSTATUSESREQUEST_H_
#define GETCONTAINERSTATUSESREQUEST_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/ContainerId.h"

using namespace std;
using namespace hadoop::yarn;
namespace libyarn {

//message GetContainerStatusesRequestProto {
//  repeated ContainerIdProto container_id = 1;
//}

class GetContainerStatusesRequest {
public:
	GetContainerStatusesRequest();
	GetContainerStatusesRequest(const GetContainerStatusesRequestProto &proto);
	virtual ~GetContainerStatusesRequest();

	GetContainerStatusesRequestProto& getProto();

	list<ContainerId> getContainerIds();
	void setContainerIds(list<ContainerId> &containerIds);
private:
	GetContainerStatusesRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETCONTAINERSTATUSESREQUEST_H_ */
