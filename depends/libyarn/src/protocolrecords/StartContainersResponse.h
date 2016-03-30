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


#ifndef STARTCONTAINERSRESPONSE_H_
#define STARTCONTAINERSRESPONSE_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/StringBytesMap.h"
#include "records/ContainerExceptionMap.h"
#include "records/ContainerId.h"

using std::string; using std::list;
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
