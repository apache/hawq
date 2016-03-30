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

#ifndef ALLOCATEREQUEST_H_
#define ALLOCATEREQUEST_H_

#include <list>

#include "YARN_yarn_service_protos.pb.h"
#include "YARN_yarn_protos.pb.h"

#include "records/ResourceRequest.h"
#include "records/ContainerId.h"
#include "records/ResourceBlacklistRequest.h"

using std::string; using std::list;
using namespace hadoop::yarn;

namespace libyarn {

/*
message AllocateRequestProto {
  repeated ResourceRequestProto ask = 1;
  repeated ContainerIdProto release = 2;
  optional ResourceBlacklistRequestProto blacklist_request = 3;
  optional int32 response_id = 4;
  optional float progress = 5;
}
*/

class AllocateRequest {
public:
	AllocateRequest();
	AllocateRequest(const AllocateRequestProto &proto);
	virtual ~AllocateRequest();

	AllocateRequestProto& getProto();

	void addAsk(ResourceRequest &ask);
	void setAsks(list<ResourceRequest> &asks);
	list<ResourceRequest> getAsks();

	void addRelease(ContainerId &release);
	void setReleases(list<ContainerId> &releases);
	list<ContainerId> getReleases();

	void setBlacklistRequest(ResourceBlacklistRequest &request);
	ResourceBlacklistRequest getBlacklistRequest();

	void setResponseId(int32_t responseId);
	int32_t getResponseId();

	void setProgress(float progress);
	float getProgress();

private:
	AllocateRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* ALLOCATEREQUEST_H_ */
