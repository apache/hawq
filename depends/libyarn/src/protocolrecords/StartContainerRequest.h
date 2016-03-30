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

#ifndef STARTCONTAINERREQUEST_H_
#define STARTCONTAINERREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"
#include "YARNSecurity.pb.h"

#include "records/ContainerLaunchContext.h"
#include "records/Token.h"

using std::string; using std::list;
using namespace hadoop::yarn;
using namespace hadoop::common;

namespace libyarn {
/*
message StartContainerRequestProto {
  optional ContainerLaunchContextProto container_launch_context = 1;
  optional hadoop.common.TokenProto container_token = 2;
}
*/
class StartContainerRequest {
public:
	StartContainerRequest();
	StartContainerRequest(const StartContainerRequestProto &proto);
	virtual ~StartContainerRequest();

	StartContainerRequestProto& getProto();

	void setContainerLaunchCtx(ContainerLaunchContext &containerLaunchCtx);
	ContainerLaunchContext getContainerLaunchCtx();

	void setContainerToken(Token &token);
	Token getContainerToken();

private:
	StartContainerRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* STARTCONTAINERREQUEST_H_ */


