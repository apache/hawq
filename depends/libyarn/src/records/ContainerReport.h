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

#ifndef CONTAINERREPORT_H_
#define CONTAINERREPORT_H_

//#include "client/Token.h"
#include "YARN_yarn_protos.pb.h"
#include "YARNSecurity.pb.h"

#include "ContainerExitStatus.h"
#include "ContainerState.h"
#include "ContainerId.h"
#include "NodeId.h"
#include "Resource.h"
#include "Priority.h"

using std::string;
using namespace hadoop::yarn;
using namespace hadoop::common;

namespace libyarn {
/*
message ContainerReportProto {
  optional ContainerIdProto id = 1;
  optional ResourceProto resource = 2;
  optional NodeIdProto nodeId = 3;
  optional PriorityProto priority = 4;
  optional int64 startTime = 5;
  optional int64 finishTime = 6;
  optional ContainerExitStatusProto container_exit_status = 7;
  optional ContainerStateProto state = 8;
  optional string diagnostics = 9 [default = "N/A"];
  optional string logUrl = 10;
}
 */
class ContainerReport {
public:
	ContainerReport();
	ContainerReport(const ContainerReportProto &proto);
	virtual ~ContainerReport();

	ContainerReportProto& getProto();

	void setId(ContainerId &id);
	ContainerId getId();

	void setResource(Resource &resource);
	Resource getResource();

	void setNodeId(NodeId &nodeId);
	NodeId getNodeId();

	void setPriority(Priority &priority);
	Priority getPriority();

	void setCreationTime(int64_t time);
	int64_t getCreationTime();

	void setFinishTime(int64_t time);
	int64_t getFinishTime();

	void setContainerExitStatus(ContainerExitStatus container_exit_status);
	ContainerExitStatus getContainerExitStatus();

	void setContainerState(ContainerState state);
	ContainerState getContainerState();

	void setDiagnostics(string &diagnostics);
	string getDiagnostics();

	void setLogUrl(string &url);
	string getLogUrl();

private:
	ContainerReportProto reportProto;
};

} /* namespace libyarn */
#endif /* CONTAINERREPORT_H_ */
