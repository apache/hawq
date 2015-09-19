/*
 * ContainerReport.h
 *
 *  Created on: Jan 5, 2015
 *      Author: weikui
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

using namespace std;
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

	void setStartTime(int64_t time);
	int64_t getStartTime();

	void setFinishTime(int64_t time);
	int64_t getFinishTime();

	void setContainerExitStatus(ContainerExitStatus container_exit_status);
	ContainerExitStatus getContainerExitStatus();

	void setContaierState(ContainerState state);
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
