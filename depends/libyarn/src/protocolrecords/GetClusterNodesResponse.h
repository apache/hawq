/*
 * GetClusterNodesResponse.h
 *
 *  Created on: Jul 18, 2014
 *      Author: bwang
 */

#ifndef GETCLUSTERNODESRESPONSE_H_
#define GETCLUSTERNODESRESPONSE_H_

#include <list>

#include "YARN_yarn_service_protos.pb.h"

#include "records/NodeReport.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {
/*
message GetClusterNodesResponseProto {
  repeated NodeReportProto nodeReports = 1;
}
 */

class GetClusterNodesResponse {
public:
	GetClusterNodesResponse();
	GetClusterNodesResponse(const GetClusterNodesResponseProto &proto);
	virtual ~GetClusterNodesResponse();

	GetClusterNodesResponseProto& getProto();

	void setNodeReports(list<NodeReport> reports);
	list<NodeReport> getNodeReports();

private:
	GetClusterNodesResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETCLUSTERNODESRESPONSE_H_ */
