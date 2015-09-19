/*
 * AllocateResponse.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */

#ifndef ALLOCATERESPONSE_H_
#define ALLOCATERESPONSE_H_

#include <list>
#include <iostream>

#include "records/AMCommand.h"
#include "records/Container.h"
#include "records/ContainerStatus.h"
#include "records/Resource.h"
#include "records/NodeReport.h"
#include "records/PreemptionMessage.h"
#include "records/NMToken.h"

#include "YARN_yarn_service_protos.pb.h"
#include "YARN_yarn_protos.pb.h"



using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message AllocateResponseProto {
  optional AMCommandProto a_m_command = 1;
  optional int32 response_id = 2;
  repeated ContainerProto allocated_containers = 3;
  repeated ContainerStatusProto completed_container_statuses = 4;
  optional ResourceProto limit = 5;
  repeated NodeReportProto updated_nodes = 6;
  optional int32 num_cluster_nodes = 7;
  optional PreemptionMessageProto preempt = 8;
  repeated NMTokenProto nm_tokens = 9;
}
*/

class AllocateResponse {
public:
	AllocateResponse();
	AllocateResponse(const AllocateResponseProto &proto);
	virtual ~AllocateResponse();

	AllocateResponseProto& getProto();

	void setAMCommand(AMCommand command);
	AMCommand getAMCommand();

	void setResponseId(int32_t responseId);
	int32_t getResponseId();

	void setAllocatedContainers(list<Container> containers);
	list<Container> getAllocatedContainers();

	void setCompletedContainerStatuses(list<ContainerStatus> statuses);
	list<ContainerStatus> getCompletedContainersStatuses();

	void setResourceLimit(Resource &limit);
	Resource getResourceLimit();

	void setUpdatedNodes(list<NodeReport> &updatedNodes);
	list<NodeReport> getUpdatedNodes();

	void setNumClusterNodes(int32_t num);
	int32_t getNumClusterNodes();

	void setPreemptionMessage(PreemptionMessage &preempt);
	PreemptionMessage getPreemptionMessage();

	void setNMTokens(list<NMToken> nmTokens);
	list<NMToken> getNMTokens();

private:
	AllocateResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* ALLOCATERESPONSE_H_ */
