/*
 * NodeReport.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */

#ifndef NODEREPORT_H_
#define NODEREPORT_H_

#include "YARN_yarn_protos.pb.h"

#include "NodeId.h"
#include "Resource.h"
#include "NodeState.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {
/*
message NodeReportProto {
  optional NodeIdProto nodeId = 1;
  optional string httpAddress = 2;
  optional string rackName = 3;
  optional ResourceProto used = 4;
  optional ResourceProto capability = 5;
  optional int32 numContainers = 6;
  optional NodeStateProto node_state = 7;
  optional string health_report = 8;
  optional int64 last_health_report_time = 9;
}
*/
class NodeReport {
public:
	NodeReport();
	NodeReport(const NodeReportProto &proto);
	virtual ~NodeReport();

	NodeReportProto& getProto();

	void setNodeId(NodeId &nodeId);
	NodeId getNodeId();

	void setHttpAddress(string &address);
	string getHttpAddress();

	void setRackName(string &name);
	string getRackName();

	void setUsedResource(Resource &res);
	Resource getUsedResource();

	void setResourceCapablity(Resource &capability);
	Resource getResourceCapability();

	void setNumContainers(int32_t num);
	int32_t getNumContainers();

	void setNodeState(NodeState state);
	NodeState getNodeState();

	void setHealthReport(string &healthReport);
	string getHealthReport();

	void setLastHealthReportTime(int64_t time);
	int64_t getLastHealthReportTime();

private:
	NodeReportProto reportProto;
};

} /* namespace libyarn */

#endif /* NODEREPORT_H_ */


