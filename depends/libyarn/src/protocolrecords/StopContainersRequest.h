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

#ifndef STOPCONTAINERSREQUEST_H_
#define STOPCONTAINERSREQUEST_H_

#include <list>

#include "YARN_yarn_service_protos.pb.h"
#include "records/ContainerId.h"

using std::string; using std::list;
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
