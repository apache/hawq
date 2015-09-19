/*
 * GetClusterNodesRequest.h
 *
 *  Created on: Jul 18, 2014
 *      Author: bwang
 */

#ifndef GETCLUSTERNODESREQUEST_H_
#define GETCLUSTERNODESREQUEST_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/NodeState.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {
/*
message GetClusterNodesRequestProto {
  repeated NodeStateProto nodeStates = 1;
}
 */
class GetClusterNodesRequest {
public:
	GetClusterNodesRequest();
	GetClusterNodesRequest(const GetClusterNodesRequestProto &proto);
	virtual ~GetClusterNodesRequest();

	GetClusterNodesRequestProto& getProto();

	void setNodeStates(list<NodeState> &states);
	list<NodeState> getNodeStates();

private:
	GetClusterNodesRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETCLUSTERNODESREQUEST_H_ */
