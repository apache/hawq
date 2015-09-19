/*
 * ContainerStatus.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */

#ifndef CONTAINERSTATUS_H_
#define CONTAINERSTATUS_H_
#include "YARN_yarn_protos.pb.h"
#include "records/ContainerState.h"
#include "records/ContainerId.h"

using namespace std;
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

	void setContaierState(ContainerState state);
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

