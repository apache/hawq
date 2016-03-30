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

#ifndef RESOURCEREQUEST_H_
#define RESOURCEREQUEST_H_

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"

#include "Priority.h"
#include "Resource.h"

using std::string;
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

