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

#ifndef CONTAINER_H_
#define CONTAINER_H_

#include "YARN_yarn_protos.pb.h"

#include "ContainerId.h"
#include "NodeId.h"
#include "Resource.h"
#include "Priority.h"
#include "Token.h"


using std::string;
using namespace hadoop::yarn;
using namespace hadoop::common;

namespace libyarn {

/*
message ContainerProto {
  optional ContainerIdProto id = 1;
  optional NodeIdProto nodeId = 2;
  optional string node_http_address = 3;
  optional ResourceProto resource = 4;
  optional PriorityProto priority = 5;
  optional hadoop.common.TokenProto container_token = 6;
}
*/

class Container {
public:
	Container();
	Container(const ContainerProto &proto);
	virtual ~Container();

	ContainerProto& getProto();

	void setId(ContainerId &id);
	ContainerId getId();

	void setNodeId(NodeId &nodeId);
	NodeId getNodeId();

	void setNodeHttpAddress(string &httpAddress);
	string getNodeHttpAddress();

	void setResource(Resource &resource);
	Resource getResource();

	void setPriority(Priority &priority);
	Priority getPriority();


	void setContainerToken(Token containerToken);
	Token getContainerToken();

private:
	ContainerProto containerProto;
};

} /* namespace libyarn */

#endif /* CONTAINER_H_ */
