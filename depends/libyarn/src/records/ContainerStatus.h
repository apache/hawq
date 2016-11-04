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

#ifndef CONTAINERSTATUS_H_
#define CONTAINERSTATUS_H_
#include "YARN_yarn_protos.pb.h"
#include "records/ContainerState.h"
#include "records/ContainerId.h"

using std::string;
using namespace hadoop::yarn;

namespace libyarn {
/*
message ContainerStatusProto {
  optional ContainerIdProto container_id = 1;
  optional ContainerStateProto state = 2;
  optional string diagnostics = 3 [default = "N/A"];
  optional int32 exit_status = 4 [default = -1000];
}
*/
class ContainerStatus {
public:
	ContainerStatus();
	ContainerStatus(const ContainerStatusProto &proto);
	virtual ~ContainerStatus();

	ContainerStatusProto& getProto();

	void setContainerId(ContainerId &containerId);
	ContainerId getContainerId();

	void setContainerState(ContainerState state);
	ContainerState getContainerState();

	void setDiagnostics(string &diagnostics);
	string getDiagnostics();

	void setExitStatus(int32_t exitStatus);
	int32_t getExitStatus();

private:
	ContainerStatusProto statusProto;
};

} /* namespace libyarn */

#endif /* CONTAINERSTATUS_H_ */

