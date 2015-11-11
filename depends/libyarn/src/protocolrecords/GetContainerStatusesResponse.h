/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
